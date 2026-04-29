package net.azisaba.itemstash.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mariadb.jdbc.Driver;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;

public class DBConnector {
    private static final Timer TIMER = new Timer(true);
    private static final @NotNull Deque<QueryData<Object>> QUERY_QUEUE = new ConcurrentLinkedDeque<>();
    private static @Nullable HikariDataSource dataSource;

    /**
     * Initializes the data source and pool.
     * @return affected rows when deleting expired stashes
     * @throws SQLException if an error occurs while initializing the pool
     */
    public static int init(@NotNull DatabaseConfig databaseConfig) throws SQLException {
        new Driver();
        HikariConfig config = new HikariConfig();
        if (databaseConfig.driver() != null) {
            config.setDriverClassName(databaseConfig.driver());
        }
        config.setJdbcUrl(databaseConfig.toUrl());
        config.setUsername(databaseConfig.username());
        config.setPassword(databaseConfig.password());
        config.setDataSourceProperties(databaseConfig.properties());
        dataSource = new HikariDataSource(config);
        createTables();
        TIMER.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (QUERY_QUEUE.isEmpty()) {
                    return;
                }
                try {
                    use(connection -> {
                        while (!QUERY_QUEUE.isEmpty()) {
                            QueryData<Object> data = QUERY_QUEUE.peek();
                            if (data == null) {
                                return;
                            }
                            //noinspection SqlSourceToSinkFlow
                            try (PreparedStatement statement = connection.prepareStatement(data.sql)) {
                                for (int i = 0; i < data.args.size(); i++) {
                                    statement.setObject(i + 1, data.args.get(i));
                                }
                                try (ResultSet rs = statement.executeQuery()) {
                                    data.future.complete(data.resultExtractor.apply(rs));
                                }
                            }
                            QUERY_QUEUE.remove();
                        }
                    });
                } catch (SQLException ignored) {
                }
            }
        }, 500, 500);
        return getPrepareStatement("DELETE FROM `stashes` WHERE `expires_at` > 0 AND `expires_at` < ?", stmt -> {
            stmt.setLong(1, System.currentTimeMillis());
            return stmt.executeUpdate();
        });
    }

    @SuppressWarnings("unchecked")
    public static <T> @NotNull CompletableFuture<T> queryWithRetry(@NotNull String sql, @NotNull List<?> values, @NotNull Function<ResultSet, T> resultExtractor) {
        QueryData<T> data = new QueryData<>(sql, values, resultExtractor);
        QUERY_QUEUE.add((QueryData<Object>) data);
        return data.future;
    }

    public static @NotNull CompletableFuture<Void> executeWithRetry(@NotNull String sql, @NotNull List<?> values) {
        return queryWithRetry(sql, values, rs -> null);
    }

    public static void createTables() throws SQLException {
        runPrepareStatement("CREATE TABLE IF NOT EXISTS `stashes` (\n" +
                "  `id` BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  `uuid` VARCHAR(36) NOT NULL,\n" +
                "  `item` MEDIUMBLOB NOT NULL,\n" +
                "  `expires_at` BIGINT NOT NULL DEFAULT -1,\n" +
                "  `true_amount` INT NOT NULL DEFAULT -1,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;", PreparedStatement::execute);
        runPrepareStatement("CREATE TABLE IF NOT EXISTS `stashes_players` (\n" +
                "  `uuid` VARCHAR(36) NOT NULL PRIMARY KEY,\n" +
                "  `operation_in_progress` BIGINT NOT NULL DEFAULT 0,\n" +
                "  `suppress_notification` TINYINT(1) NOT NULL DEFAULT 0" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;", PreparedStatement::execute);
        ensureStashesSchema();
    }

    private static void ensureStashesSchema() throws SQLException {
        use(connection -> {
            if (!hasColumn(connection, "stashes", "id")) {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("ALTER TABLE `stashes` ADD COLUMN `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST");
                }
            }
            if (!hasPrimaryKey(connection, "stashes")) {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("ALTER TABLE `stashes` ADD PRIMARY KEY (`id`)");
                }
            }
            ensureIndex(connection, "stashes", "idx_stashes_uuid_expires_id",
                    "CREATE INDEX `idx_stashes_uuid_expires_id` ON `stashes` (`uuid`, `expires_at`, `id`)");
            ensureIndex(connection, "stashes", "idx_stashes_uuid_id",
                    "CREATE INDEX `idx_stashes_uuid_id` ON `stashes` (`uuid`, `id`)");
        });
    }

    private static boolean hasColumn(@NotNull Connection connection, @NotNull String tableName, @NotNull String columnName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tableName, columnName)) {
            return rs.next();
        }
    }

    private static boolean hasPrimaryKey(@NotNull Connection connection, @NotNull String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(connection.getCatalog(), null, tableName)) {
            return rs.next();
        }
    }

    private static void ensureIndex(@NotNull Connection connection, @NotNull String tableName, @NotNull String indexName, @Language("SQL") @NotNull String createIndexSql) throws SQLException {
        if (hasIndex(connection, tableName, indexName)) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createIndexSql);
        }
    }

    private static boolean hasIndex(@NotNull Connection connection, @NotNull String tableName, @NotNull String indexName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getIndexInfo(connection.getCatalog(), null, tableName, false, false)) {
            while (rs.next()) {
                if (indexName.equalsIgnoreCase(rs.getString("INDEX_NAME"))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the data source. Throws an exception if the data source is not initialized using {@link #init(DatabaseConfig)}.
     * @return the data source
     * @throws NullPointerException if the data source is not initialized using {@link #init(DatabaseConfig)}
     */
    @Contract(pure = true)
    @NotNull
    public static HikariDataSource getDataSource() {
        return Objects.requireNonNull(dataSource, "#init was not called");
    }

    @NotNull
    public static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    @Contract(pure = true)
    public static <R> R use(@NotNull SQLThrowableFunction<Connection, R> action) throws SQLException {
        try (Connection connection = getConnection()) {
            return action.apply(connection);
        }
    }

    @Contract(pure = true)
    public static void use(@NotNull SQLThrowableConsumer<Connection> action) throws SQLException {
        try (Connection connection = getConnection()) {
            action.accept(connection);
        }
    }

    @Contract(pure = true)
    public static void runPrepareStatement(@Language("SQL") @NotNull String sql, @NotNull SQLThrowableConsumer<PreparedStatement> action) throws SQLException {
        use(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                action.accept(statement);
            }
        });
    }

    @Contract(pure = true)
    public static <R> R getPrepareStatement(@Language("SQL") @NotNull String sql, @NotNull SQLThrowableFunction<PreparedStatement, R> action) throws SQLException {
        return use(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                return action.apply(statement);
            }
        });
    }

    @Contract(pure = true)
    public static void useStatement(@NotNull SQLThrowableConsumer<Statement> action) throws SQLException {
        use(connection -> {
            try (Statement statement = connection.createStatement()) {
                action.accept(statement);
            }
        });
    }

    public static boolean isOperationInProgress(@NotNull UUID uuid) throws SQLException {
        return getPrepareStatement("SELECT `operation_in_progress` FROM `stashes_players` WHERE `uuid` = ?", ps -> {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("operation_in_progress") > System.currentTimeMillis();
                } else {
                    return false;
                }
            }
        });
    }

    public static void setOperationInProgress(@NotNull UUID uuid, boolean flag) throws SQLException {
        if (flag) {
            setOperationInProgress(uuid);
        } else {
            setOperationInProgress(uuid, 0);
        }
    }

    public static void setOperationInProgress(@NotNull UUID uuid) throws SQLException {
        setOperationInProgress(uuid, System.currentTimeMillis() + 1000 * 60 * 30);
    }

    public static void setOperationInProgress(@NotNull UUID uuid, long expiresAt) throws SQLException {
        runPrepareStatement("INSERT INTO `stashes_players` (`uuid`, `operation_in_progress`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `operation_in_progress` = VALUES(`operation_in_progress`)", ps -> {
            ps.setString(1, uuid.toString());
            ps.setLong(2, expiresAt);
            ps.executeUpdate();
        });
    }

    public static boolean isSuppressNotification(@NotNull UUID uuid) throws SQLException {
        return getPrepareStatement("SELECT `suppress_notification` FROM `stashes_players` WHERE `uuid` = ?", ps -> {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean("suppress_notification");
                } else {
                    return false;
                }
            }
        });
    }

    public static void setSuppressNotification(@NotNull UUID uuid, boolean flag) throws SQLException {
        runPrepareStatement("INSERT INTO `stashes_players` (`uuid`, `suppress_notification`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `suppress_notification` = VALUES(`suppress_notification`)", ps -> {
            ps.setString(1, uuid.toString());
            ps.setBoolean(2, flag);
            ps.executeUpdate();
        });
    }

    /**
     * Closes the data source if it is initialized.
     */
    public static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
