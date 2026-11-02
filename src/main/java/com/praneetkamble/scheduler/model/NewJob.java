package com.praneetkamble.scheduler.model;

import java.time.Instant;
import java.util.Objects;

/**
 * A request to enqueue a job.
 *
 * @param runAt when to first run; {@code null} means "now" (or the next cron occurrence)
 * @param cron  optional five-field cron expression making the job recurring
 */
public record NewJob(String type, String payload, Instant runAt, int maxAttempts, String cron) {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    public NewJob {
        Objects.requireNonNull(type, "type");
        if (type.isBlank() || type.length() > 100) {
            throw new IllegalArgumentException("type must be 1-100 characters");
        }
        payload = payload == null ? "{}" : payload;
        if (maxAttempts < 1 || maxAttempts > 100) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and 100");
        }
    }

    public static NewJob of(String type, String payload) {
        return new NewJob(type, payload, null, DEFAULT_MAX_ATTEMPTS, null);
    }
}
