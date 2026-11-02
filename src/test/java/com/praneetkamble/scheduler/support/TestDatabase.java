package com.praneetkamble.scheduler.support;

import com.praneetkamble.scheduler.store.Database;
import com.zaxxer.hikari.HikariDataSource;
import java.util.UUID;

public final class TestDatabase {

    private TestDatabase() {
    }

    /** A fresh, isolated in-memory H2 database with the schema applied. */
    public static HikariDataSource create() {
        return Database.connect("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", null, null, 10);
    }
}
