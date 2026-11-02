package com.praneetkamble.scheduler.cron;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CronExpressionTest {

    private static ZonedDateTime utc(int y, int mo, int d, int h, int mi) {
        return ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC);
    }

    private static ZonedDateTime next(String cron, ZonedDateTime from) {
        return CronExpression.parse(cron).nextAfter(from);
    }

    @Test
    void everyMinuteAdvancesToNextMinute() {
        assertThat(next("* * * * *", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 3, 1, 10, 16));
    }

    @Test
    void nextIsStrictlyAfterEvenWithSeconds() {
        ZonedDateTime from = utc(2026, 3, 1, 10, 15).plusSeconds(30);
        assertThat(next("* * * * *", from)).isEqualTo(utc(2026, 3, 1, 10, 16));
        assertThat(next("15 10 * * *", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 3, 2, 10, 15));
    }

    @Test
    void stepOverWildcard() {
        assertThat(next("*/15 * * * *", utc(2026, 3, 1, 10, 16))).isEqualTo(utc(2026, 3, 1, 10, 30));
        assertThat(next("*/15 * * * *", utc(2026, 3, 1, 10, 46))).isEqualTo(utc(2026, 3, 1, 11, 0));
    }

    @Test
    void rangesListsAndRangeSteps() {
        assertThat(next("0 9-17/4 * * *", utc(2026, 3, 1, 9, 0))).isEqualTo(utc(2026, 3, 1, 13, 0));
        assertThat(next("5,35 * * * *", utc(2026, 3, 1, 10, 6))).isEqualTo(utc(2026, 3, 1, 10, 35));
    }

    @Test
    void dailyAtMidnightRollsOverMonthAndYear() {
        assertThat(next("0 0 * * *", utc(2026, 12, 31, 23, 59))).isEqualTo(utc(2027, 1, 1, 0, 0));
    }

    @Test
    void weekdayNamesAndNumbers() {
        // 2026-03-01 is a Sunday
        assertThat(next("30 8 * * MON", utc(2026, 3, 1, 0, 0))).isEqualTo(utc(2026, 3, 2, 8, 30));
        assertThat(next("0 12 * * 0", utc(2026, 3, 1, 13, 0))).isEqualTo(utc(2026, 3, 8, 12, 0));
        assertThat(next("0 12 * * 7", utc(2026, 3, 1, 13, 0))).isEqualTo(utc(2026, 3, 8, 12, 0));
        assertThat(next("0 6 * * MON-FRI", utc(2026, 3, 6, 7, 0))).isEqualTo(utc(2026, 3, 9, 6, 0));
    }

    @Test
    void monthNames() {
        assertThat(next("0 0 1 JAN,JUL *", utc(2026, 3, 1, 0, 0))).isEqualTo(utc(2026, 7, 1, 0, 0));
    }

    @Test
    void dayOfMonthAndDayOfWeekAreOredWhenBothRestricted() {
        // 13th of the month OR any Friday. 2026-03-01 is Sunday -> first Friday is the 6th.
        assertThat(next("0 0 13 * FRI", utc(2026, 3, 1, 0, 0))).isEqualTo(utc(2026, 3, 6, 0, 0));
        assertThat(next("0 0 13 * FRI", utc(2026, 3, 7, 0, 0))).isEqualTo(utc(2026, 3, 13, 0, 0));
    }

    @Test
    void leapDayIsFoundAcrossYears() {
        assertThat(next("0 0 29 2 *", utc(2026, 3, 1, 0, 0))).isEqualTo(utc(2028, 2, 29, 0, 0));
    }

    @Test
    void macros() {
        assertThat(next("@hourly", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 3, 1, 11, 0));
        assertThat(next("@daily", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 3, 2, 0, 0));
        assertThat(next("@weekly", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 3, 8, 0, 0));
        assertThat(next("@monthly", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2026, 4, 1, 0, 0));
        assertThat(next("@yearly", utc(2026, 3, 1, 10, 15))).isEqualTo(utc(2027, 1, 1, 0, 0));
    }

    @Test
    void instantOverloadWorksInUtc() {
        Instant from = Instant.parse("2026-03-01T10:15:00Z");
        assertThat(CronExpression.parse("0 12 * * *").nextAfter(from)).isEqualTo(Instant.parse("2026-03-01T12:00:00Z"));
    }

    @Test
    void matchesChecksAllFields() {
        CronExpression cron = CronExpression.parse("30 8 * * *");
        assertThat(cron.matches(utc(2026, 3, 1, 8, 30))).isTrue();
        assertThat(cron.matches(utc(2026, 3, 1, 8, 31))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *",
            "* * * 13 *", "* * * * 8", "5-1 * * * *", "*/0 * * * *", "a * * * *", "@never", "1-2-3 * * * *"})
    void rejectsInvalidExpressions(String expression) {
        assertThat(CronExpression.isValid(expression)).isFalse();
        assertThatThrownBy(() -> CronExpression.parse(expression)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNull() {
        assertThat(CronExpression.isValid(null)).isFalse();
    }

    @Test
    void impossibleDateIsReportedInsteadOfLoopingForever() {
        assertThatThrownBy(() -> next("0 0 31 2 *", utc(2026, 1, 1, 0, 0))).isInstanceOf(IllegalStateException.class);
    }
}
