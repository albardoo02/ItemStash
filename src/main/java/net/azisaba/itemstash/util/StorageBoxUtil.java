package net.azisaba.itemstash.util;

import org.bukkit.Bukkit;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;
import xyz.acrylicstyle.storageBox.utils.StorageBox;
import xyz.acrylicstyle.storageBox.utils.StorageBoxUtils;

import java.util.Map;

public class StorageBoxUtil {
    /**
     * Try to store item to storage box.
     * @param inventory the inventory to find storage box from
     * @param stack the item
     * @return true if the item is stored to storage box, false otherwise
     */
    public static boolean tryAddItemToStorageBox(@NotNull Inventory inventory, @NotNull ItemStack stack) {
        if (Bukkit.getPluginManager().isPluginEnabled("StorageBox")) {
            return addItemToStorageBox(inventory, stack);
        }
        return false;
    }

    private static boolean addItemToStorageBox(@NotNull Inventory inventory, @NotNull ItemStack stack) {
        Map.Entry<Integer, StorageBox> entry = StorageBoxUtils.getStorageBoxForType(inventory, stack);
        if (entry == null) return false;
        StorageBox storageBox = entry.getValue();
        storageBox.setAmount(storageBox.getAmount() + stack.getAmount());
        inventory.setItem(entry.getKey(), storageBox.getItemStack());
        return true;
    }
}
