package net.azisaba.itemstash.sql;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class QueryData<R> {
    public final String sql;
    public final List<?> args;
    public final Function<ResultSet, R> resultExtractor;
    public final CompletableFuture<R> future = new CompletableFuture<>();

    public QueryData(String sql, List<?> args, Function<ResultSet, R> resultExtractor) {
        this.sql = sql;
        this.args = args;
        this.resultExtractor = resultExtractor;
    }

    public QueryData(String sql, List<?> args) {
        this(sql, args, rs -> null);
    }
}
