package com.praneetkamble.scheduler.model;

import java.time.Instant;

/**
 * Immutable snapshot of a job row.
 *
 * @param attempts  number of times execution has been started for the current run
 * @param runCount  number of completed runs (relevant for recurring jobs)
 * @param cron      cron expression for recurring jobs, otherwise {@code null}
 */
public record Job(
        String id,
        String type,
        String payload,
        JobStatus status,
        Instant runAt,
        int attempts,
        int maxAttempts,
        String cron,
        long runCount,
        String lastError,
        String lockedBy,
        Instant lockedUntil,
        Instant createdAt,
        Instant updatedAt) {

    public boolean isRecurring() {
        return cron != null;
    }
}
