package net.azisaba.itemstash.gui;

import net.azisaba.itemstash.ItemStash;
import net.azisaba.itemstash.ItemStashPlugin;
import net.azisaba.itemstash.command.PickupStashCommand;
import net.azisaba.itemstash.sql.DBConnector;
import net.azisaba.itemstash.util.ItemUtil;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PickupStashScreen implements InventoryHolder {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private final Inventory inventory = Bukkit.createInventory(this, 54, "Stash回収");
    private final UUID stashOwner;
    private final List<DisplayEntry> items;
    private int page = 0;
    private boolean acceptingClick = true;

    public PickupStashScreen(@NotNull UUID stashOwner, @NotNull List<StashEntry> items) {
        this.stashOwner = stashOwner;
        this.items = flatten(items);
        initInventory();
    }

    private static @NotNull List<DisplayEntry> flatten(@NotNull List<StashEntry> items) {
        List<DisplayEntry> flattened = new ArrayList<>();
        for (StashEntry item : items) {
            Optional<DisplayEntry> opt = flattened.stream().filter(is -> is.item.isSimilar(item.item)).findAny();
            if (opt.isPresent()) {
                DisplayEntry entry = opt.get();
                entry.item.setAmount(entry.item.getAmount() + item.item.getAmount());
                if (entry.expiresAt == -1 || (item.expiresAt != -1 && entry.expiresAt > item.expiresAt)) {
                    entry.expiresAt = item.expiresAt;
                }
                entry.stashIds.add(item.id);
            } else {
                flattened.add(new DisplayEntry(item));
            }
        }
        return flattened;
    }

    public void initInventory() {
        inventory.clear();
        for (int i = 0; i < items.subList(page * 45, Math.min((page + 1) * 45, items.size())).size(); i++) {
            DisplayEntry entry = items.get(page * 45 + i);
            ItemStack screenItem = entry.item.clone();
            if (screenItem.getItemMeta() == null) {
                ((ItemStashPlugin) ItemStash.getInstance()).getSLF4JLogger().info("{} (#{}) does not have item meta", screenItem, i);
                continue;
            }
            long expiresAt = entry.expiresAt;
            List<String> lore = screenItem.getLore();
            if (lore == null) {
                lore = new ArrayList<>();
            } else {
                lore.add("");
            }
            lore.add(ChatColor.GOLD.toString() + ChatColor.BOLD + "Stashに格納されているアイテム数: " + ChatColor.GREEN + ChatColor.BOLD + screenItem.getAmount());
            if (expiresAt > 0) {
                lore.add(ChatColor.GOLD + "直近の有効期限: " + ChatColor.RED + DATE_FORMAT.format(expiresAt));
            }
            screenItem.setLore(lore);
            inventory.setItem(i, screenItem);
        }
        ItemStack previousPage = new ItemStack(Material.ARROW);
        ItemMeta meta1 = previousPage.getItemMeta();
        meta1.setDisplayName(ChatColor.GOLD + "前のページ");
        previousPage.setItemMeta(meta1);
        inventory.setItem(45, previousPage);
        ItemStack nextPage = new ItemStack(Material.ARROW);
        ItemMeta meta2 = nextPage.getItemMeta();
        meta2.setDisplayName(ChatColor.GOLD + "次のページ");
        nextPage.setItemMeta(meta2);
        inventory.setItem(53, nextPage);
    }

    @Override
    public @NotNull Inventory getInventory() {
        return inventory;
    }

    public static class StashEntry {
        private final long id;
        private final ItemStack item;
        private final long expiresAt;

        public StashEntry(long id, @NotNull ItemStack item, long expiresAt) {
            this.id = id;
            this.item = item;
            this.expiresAt = expiresAt;
        }
    }

    private static class DisplayEntry {
        private final ItemStack item;
        private long expiresAt;
        private final List<Long> stashIds = new ArrayList<Long>();

        private DisplayEntry(@NotNull StashEntry entry) {
            this.item = entry.item.clone();
            this.expiresAt = entry.expiresAt;
            this.stashIds.add(entry.id);
        }
    }

    public static class EventListener implements Listener {
        private final ItemStashPlugin plugin;

        public EventListener(@NotNull ItemStashPlugin plugin) {
            this.plugin = plugin;
        }

        @EventHandler
        public void onInventoryDrag(InventoryDragEvent e) {
            if (e.getInventory().getHolder() instanceof PickupStashScreen) {
                e.setCancelled(true);
            }
        }

        @EventHandler
        public void onInventoryClick(InventoryClickEvent e) {
            if (!(e.getInventory().getHolder() instanceof PickupStashScreen)) {
                return;
            }
            e.setCancelled(true);
            if (e.getClickedInventory() == null || !(e.getClickedInventory().getHolder() instanceof PickupStashScreen)) {
                return;
            }
            PickupStashScreen screen = (PickupStashScreen) e.getInventory().getHolder();
            if (!screen.acceptingClick) {
                return;
            }
            if (e.getSlot() == 45) {
                if (screen.page > 0) {
                    screen.page--;
                    screen.initInventory();
                }
                return;
            }
            if (e.getSlot() == 53) {
                if (screen.page < screen.items.size() / 45) {
                    screen.page++;
                    screen.initInventory();
                }
                return;
            }
            if (e.getSlot() >= 45 || e.getCurrentItem() == null || e.getCurrentItem().getType() == Material.AIR) {
                return;
            }
            PickupStashCommand.PROCESSING.add(e.getWhoClicked().getUniqueId());
            DisplayEntry displayEntry = screen.items.get(screen.page * 45 + e.getSlot());
            ItemStack stack = displayEntry.item.clone();
            int originalAmount = stack.getAmount();
            screen.acceptingClick = false;
            Bukkit.getScheduler().runTask(plugin, () -> {
                e.getWhoClicked().closeInventory();
                e.getWhoClicked().sendMessage(ChatColor.GRAY + "処理中です...");
                plugin.getLogger().info("Attempting to take " + stack + " from stash of " + e.getWhoClicked().getName());
                long start = System.currentTimeMillis();
                Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                    try (Connection connection = DBConnector.getConnection()) {
                        DBConnector.setOperationInProgress(screen.stashOwner, true);
                        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM `stashes` WHERE `id` = ?")) {
                            for (Long stashId : displayEntry.stashIds) {
                                stmt.setLong(1, stashId);
                                stmt.addBatch();
                            }
                            stmt.executeBatch();
                        }
                    } catch (SQLException ex) {
                        PickupStashCommand.PROCESSING.remove(e.getWhoClicked().getUniqueId());
                        try {
                            DBConnector.setOperationInProgress(screen.stashOwner, false);
                        } catch (SQLException exc) {
                            exc.addSuppressed(ex);
                            throw new RuntimeException(exc);
                        }
                        throw new RuntimeException(ex);
                    }
                    Bukkit.getScheduler().runTask(plugin, () -> {
                        Collection<ItemStack> items =
                                ((Player) e.getWhoClicked()).isOnline() ? ItemUtil.addItem(e.getWhoClicked().getInventory(), stack).values() : Collections.singleton(stack);
                        if (items.isEmpty()) {
                            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                                PickupStashCommand.PROCESSING.remove(e.getWhoClicked().getUniqueId());
                                try {
                                    DBConnector.setOperationInProgress(screen.stashOwner, false);
                                } catch (SQLException ex) {
                                    plugin.getSLF4JLogger().error("Failed to set operation_in_progress state", ex);
                                }
                                long elapsed = System.currentTimeMillis() - start;
                                e.getWhoClicked().sendMessage(ChatColor.GREEN + "すべてのアイテム(" + originalAmount + "個)を受け取りました。" + ChatColor.DARK_GRAY + " [" + elapsed + "ms]");
                            });
                        } else {
                            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                                try {
                                    int amount = 0;
                                    for (ItemStack item : items) {
                                        plugin.addItemToStash(screen.stashOwner, item);
                                        amount += item.getAmount();
                                    }
                                    long elapsed = System.currentTimeMillis() - start;
                                    e.getWhoClicked().sendMessage(ChatColor.GOLD.toString() + amount + ChatColor.RED + "個のアイテムが受け取れませんでした。" + ChatColor.DARK_GRAY + " [" + elapsed + "ms]");
                                } finally {
                                    PickupStashCommand.PROCESSING.remove(e.getWhoClicked().getUniqueId());
                                    try {
                                        DBConnector.setOperationInProgress(screen.stashOwner, false);
                                    } catch (SQLException ex) {
                                        plugin.getSLF4JLogger().error("Failed to set operation_in_progress state", ex);
                                    }
                                }
                            });
                        }
                    });
                });
            });
        }
    }
}
