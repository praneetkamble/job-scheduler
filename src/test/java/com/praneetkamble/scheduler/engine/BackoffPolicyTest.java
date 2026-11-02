package com.praneetkamble.scheduler.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    private final BackoffPolicy noJitter =
            new BackoffPolicy(Duration.ofSeconds(2), Duration.ofSeconds(60), 0, () -> 0.5);

    @Test
    void doublesEveryAttempt() {
        assertThat(noJitter.delayAfter(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(noJitter.delayAfter(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(noJitter.delayAfter(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(noJitter.delayAfter(5)).isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    void isCapped() {
        assertThat(noJitter.delayAfter(6)).isEqualTo(Duration.ofSeconds(60));
        assertThat(noJitter.delayAfter(500)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void jitterOnlyShortensTheDelay() {
        BackoffPolicy full = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofMinutes(10), 0.5, () -> 1.0);
        BackoffPolicy none = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofMinutes(10), 0.5, () -> 0.0);
        assertThat(full.delayAfter(1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(none.delayAfter(1)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void delayIsNeverZero() {
        BackoffPolicy tiny = new BackoffPolicy(Duration.ofMillis(1), Duration.ofMillis(1), 1, () -> 1.0);
        assertThat(tiny.delayAfter(1)).isPositive();
    }

    @Test
    void rejectsBadArguments() {
        assertThatThrownBy(() -> noJitter.delayAfter(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, Duration.ofSeconds(1), 0, () -> 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(5), Duration.ofSeconds(1), 0, () -> 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(1), Duration.ofSeconds(5), 1.5, () -> 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
