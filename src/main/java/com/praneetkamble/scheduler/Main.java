package com.praneetkamble.scheduler;

import com.praneetkamble.scheduler.api.ApiServer;
import com.praneetkamble.scheduler.engine.BackoffPolicy;
import com.praneetkamble.scheduler.engine.JobScheduler;
import com.praneetkamble.scheduler.engine.SchedulerConfig;
import com.praneetkamble.scheduler.handler.HandlerRegistry;
import com.praneetkamble.scheduler.store.Database;
import com.praneetkamble.scheduler.store.JdbcJobStore;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point. Configuration comes from environment variables:
 * {@code PORT}, {@code JDBC_URL}, {@code DB_USER}, {@code DB_PASSWORD}, {@code WORKERS}, {@code POLL_MS},
 * {@code BACKOFF_BASE_MS}, {@code BACKOFF_MAX_MS} and {@code WORKER_ID}.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = intEnv("PORT", 8080);
        String jdbcUrl = env("JDBC_URL", "jdbc:h2:file:./data/scheduler;AUTO_SERVER=TRUE");
        int workers = intEnv("WORKERS", 4);

        HikariDataSource dataSource = Database.connect(jdbcUrl, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"),
                workers + 4);
        SchedulerConfig config = new SchedulerConfig(
                env("WORKER_ID", "worker-" + UUID.randomUUID().toString().substring(0, 8)),
                workers,
                Duration.ofMillis(intEnv("POLL_MS", 500)),
                Duration.ofMinutes(5),
                BackoffPolicy.exponential(Duration.ofMillis(intEnv("BACKOFF_BASE_MS", 2000)),
                        Duration.ofMillis(intEnv("BACKOFF_MAX_MS", 3_600_000))));

        HandlerRegistry registry = HandlerRegistry.withBuiltIns();
        Clock clock = Clock.systemUTC();
        JobScheduler scheduler = new JobScheduler(new JdbcJobStore(dataSource), registry, config, clock);
        ApiServer api = new ApiServer(scheduler, registry.types(), clock, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            api.close();
            scheduler.shutdown(Duration.ofSeconds(15));
            dataSource.close();
        }, "shutdown"));

        scheduler.start();
        api.start();
        log.info("Dashboard: http://localhost:{}/", api.port());
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static int intEnv(String name, int fallback) {
        return Integer.parseInt(env(name, String.valueOf(fallback)));
    }
}
