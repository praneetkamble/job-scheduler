package com.praneetkamble.scheduler.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/** Creates the connection pool and applies the schema. Works with H2 and PostgreSQL. */
public final class Database {

    private Database() {
    }

    public static HikariDataSource connect(String jdbcUrl, String user, String password, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        if (user != null) {
            config.setUsername(user);
        }
        if (password != null) {
            config.setPassword(password);
        }
        config.setMaximumPoolSize(maxPoolSize);
        config.setPoolName("scheduler-db");
        HikariDataSource ds = new HikariDataSource(config);
        migrate(ds);
        return ds;
    }

    /** Executes {@code schema.sql}; every statement is idempotent. */
    public static void migrate(DataSource ds) {
        String sql;
        try (InputStream in = Database.class.getResourceAsStream("/schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("schema.sql not found on classpath");
            }
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            for (String statement : sql.split(";")) {
                if (!statement.isBlank()) {
                    st.execute(statement);
                }
            }
        } catch (SQLException e) {
            throw new StoreException("Failed to apply schema", e);
        }
    }
}
