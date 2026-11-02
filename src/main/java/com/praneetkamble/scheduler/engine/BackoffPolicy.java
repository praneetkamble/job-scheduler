package com.praneetkamble.scheduler.engine;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with a cap and optional jitter: {@code min(max, base * 2^(attempt-1))}, reduced by up
 * to {@code jitter} (a fraction between 0 and 1) so that many jobs failing together do not retry in lock-step.
 */
public final class BackoffPolicy {

    private final Duration base;
    private final Duration max;
    private final double jitter;
    private final DoubleSupplier random;

    public BackoffPolicy(Duration base, Duration max, double jitter, DoubleSupplier random) {
        if (base.isNegative() || base.isZero() || max.compareTo(base) < 0) {
            throw new IllegalArgumentException("require 0 < base <= max");
        }
        if (jitter < 0 || jitter > 1) {
            throw new IllegalArgumentException("jitter must be within [0, 1]");
        }
        this.base = base;
        this.max = max;
        this.jitter = jitter;
        this.random = random;
    }

    public static BackoffPolicy exponential(Duration base, Duration max) {
        return new BackoffPolicy(base, max, 0.2, Math::random);
    }

    /** Delay before the retry that follows the given (1-based) failed attempt. */
    public Duration delayAfter(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be >= 1");
        }
        long exponent = Math.min(failedAttempt - 1, 40); // beyond this the cap always applies
        double raw = (double) base.toMillis() * Math.pow(2, exponent);
        double capped = Math.min(raw, (double) max.toMillis());
        double jittered = capped * (1.0 - jitter * random.getAsDouble());
        return Duration.ofMillis(Math.max(1, Math.round(jittered)));
    }
}
