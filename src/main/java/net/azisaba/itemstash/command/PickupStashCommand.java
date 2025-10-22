package net.azisaba.itemstash.command;

import net.azisaba.itemstash.ItemStashPlugin;
import net.azisaba.itemstash.gui.PickupStashScreen;
import net.azisaba.itemstash.sql.DBConnector;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabExecutor;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.scheduler.BukkitRunnable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PickupStashCommand implements TabExecutor {
    public /* internal */ static final Set<UUID> PROCESSING = Collections.synchronizedSet(new HashSet<>());
    private final ItemStashPlugin plugin;
    private static final int MAX_LOOPS = 20;

    public PickupStashCommand(@NotNull ItemStashPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (!(sender instanceof Player)) {
            sender.sendMessage("このコマンドはコンソールから実行できません。");
            return true;
        }
        Player player = (Player) sender;
        if (args.length == 1 && args[0].equalsIgnoreCase("nogui")) {
            if (PROCESSING.contains(player.getUniqueId())) {
                player.sendMessage(ChatColor.RED + "前回の処理が継続中です。しばらくしてからお試しください。(Local)");
                return true;
            }
            PROCESSING.add(player.getUniqueId());
            AtomicLong totalTime = new AtomicLong(0);
            new BukkitRunnable() {
                int loops = 0;
                @Override
                public void run() {
                    long start = System.currentTimeMillis();
                    try {
                        if (loops == 0) {
                            if (DBConnector.isOperationInProgress(player.getUniqueId())) {
                                player.sendMessage(ChatColor.RED + "前回の処理が継続中です。しばらくしてからお試しください。(Server)");
                                finishProcessing();
                                return;
                            }
                            player.sendMessage(ChatColor.GRAY + "処理中です...");
                            DBConnector.setOperationInProgress(player.getUniqueId(), true);
                        }
                        plugin.dumpStash(player).thenAcceptAsync(result -> {
                            totalTime.addAndGet(System.currentTimeMillis() - start);
                            loops++;

                            if (result) {
                                int remaining = plugin.getStashItemCount(player.getUniqueId());
                                if (remaining > 0 && loops < MAX_LOOPS) {
                                    player.sendMessage(ChatColor.GRAY + "追加でアイテムを受け取っています... (" + remaining + "件残り)");
                                    this.run();
                                } else {
                                    if (remaining > 0) {
                                        player.sendMessage(ChatColor.YELLOW + "一度に受け取れる上限に達しました。インベントリを整理して再度実行してください。");
                                    }
                                    player.sendMessage(ChatColor.GREEN + "アイテムをすべて受け取りました。" + ChatColor.DARK_GRAY + " [" + totalTime.get() + "ms, " + loops + "回処理]");
                                    finishProcessing();
                                }
                            } else {
                                player.sendMessage(ChatColor.RED + "一部のアイテムを受け取れませんでした。" + ChatColor.DARK_GRAY + " [" + totalTime.get() + "ms, " + loops + "回処理]");
                                finishProcessing();
                            }
                        });
                    } catch (SQLException e) {
                        player.sendMessage(ChatColor.RED + "データベースエラーが発生しました。");
                        finishProcessing();
                        throw new RuntimeException(e);
                    }
                }

                private void finishProcessing() {
                    PROCESSING.remove(player.getUniqueId());
                    try {
                        DBConnector.setOperationInProgress(player.getUniqueId(), false);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

            }.runTaskAsynchronously(plugin);
            return true;
        }
        UUID targetUUID = player.getUniqueId();
        if (sender.hasPermission("itemstash.others") && args.length >= 1) {
            targetUUID = UUID.fromString(args[0]);
        }
        if (PROCESSING.contains(targetUUID)) {
            player.sendMessage(ChatColor.RED + "前回の処理が継続中です。しばらくしてからお試しください。(Local)");
            return true;
        }
        PROCESSING.add(targetUUID);
        UUID finalTargetUUID = targetUUID;
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try (Connection connection = DBConnector.getConnection()) {
                if (DBConnector.isOperationInProgress(finalTargetUUID)) {
                    player.sendMessage(ChatColor.RED + "前回の処理が継続中です。しばらくしてからお試しください。(Server)");
                    return;
                }
                player.sendMessage(ChatColor.GRAY + "処理中です...");
                List<Map.Entry<ItemStack, Long>> items = new ArrayList<>();
                try (PreparedStatement stmt = connection.prepareStatement("SELECT `item`, `expires_at`, `true_amount` FROM `stashes` WHERE `uuid` = ?")) {
                    stmt.setString(1, finalTargetUUID.toString());
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            Blob blob = rs.getBlob("item");
                            byte[] bytes = blob.getBytes(1, (int) blob.length());
                            long expiresAt = rs.getLong("expires_at");
                            int trueAmount = rs.getInt("true_amount");
                            ItemStack item = ItemStack.deserializeBytes(bytes);
                            if (trueAmount > 0) {
                                item.setAmount(trueAmount);
                            }
                            //plugin.getLogger().info("Item: " + item);
                            items.add(new AbstractMap.SimpleImmutableEntry<>(item, expiresAt));
                        }
                    }
                }
                Bukkit.getScheduler().runTask(plugin, () -> player.openInventory(new PickupStashScreen(items).getInventory()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                PROCESSING.remove(finalTargetUUID);
            }
        });
        return true;
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String alias, @NotNull String[] args) {
        if (args.length == 1) {
            return Stream.of("nogui").filter(s -> s.startsWith(args[0])).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}