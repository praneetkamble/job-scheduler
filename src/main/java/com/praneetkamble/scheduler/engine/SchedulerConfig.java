package com.praneetkamble.scheduler.engine;

import java.time.Duration;
import java.util.UUID;

/**
 * Tunables for a {@link JobScheduler}.
 *
 * @param workers       size of the worker thread pool (= max concurrent jobs per scheduler instance)
 * @param pollInterval  how often the database is polled for due jobs
 * @param lease         how long a claimed job may run before other schedulers consider the worker dead
 */
public record SchedulerConfig(String workerId, int workers, Duration pollInterval, Duration lease,
                              BackoffPolicy backoff) {

    public SchedulerConfig {
        if (workers < 1) {
            throw new IllegalArgumentException("workers must be >= 1");
        }
        if (pollInterval.isNegative() || pollInterval.isZero() || lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("pollInterval and lease must be positive");
        }
    }

    public static SchedulerConfig defaults() {
        return new SchedulerConfig("worker-" + UUID.randomUUID().toString().substring(0, 8), 4,
                Duration.ofMillis(500), Duration.ofMinutes(5),
                BackoffPolicy.exponential(Duration.ofSeconds(2), Duration.ofHours(1)));
    }
}
