package com.praneetkamble.scheduler.cron;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A parser and evaluator for standard five-field cron expressions
 * ({@code minute hour day-of-month month day-of-week}).
 *
 * <p>Supported syntax per field: {@code *}, single values, lists ({@code 1,5,9}), ranges ({@code 1-5}),
 * steps ({@code *}{@code /15}, {@code 10-30/5}, {@code 5/10}), month names ({@code JAN-DEC}) and weekday names
 * ({@code SUN-SAT}). Weekday {@code 0} and {@code 7} both mean Sunday. The macros {@code @hourly},
 * {@code @daily}, {@code @weekly}, {@code @monthly} and {@code @yearly} are accepted.
 *
 * <p>As in Vixie cron, when both day-of-month and day-of-week are restricted, a date matches if
 * <em>either</em> of them matches.
 */
public final class CronExpression {

    private static final Map<String, String> MACROS = Map.of(
            "@hourly", "0 * * * *",
            "@daily", "0 0 * * *",
            "@midnight", "0 0 * * *",
            "@weekly", "0 0 * * 0",
            "@monthly", "0 0 1 * *",
            "@yearly", "0 0 1 1 *",
            "@annually", "0 0 1 1 *");

    private static final List<String> MONTHS = List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");
    private static final List<String> DAYS = List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");

    private static final int SEARCH_LIMIT_YEARS = 8;

    private final String expression;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean dayOfMonthWildcard;
    private final boolean dayOfWeekWildcard;

    private CronExpression(String expression, BitSet minutes, BitSet hours, BitSet daysOfMonth, BitSet months,
                           BitSet daysOfWeek, boolean dayOfMonthWildcard, boolean dayOfWeekWildcard) {
        this.expression = expression;
        this.minutes = minutes;
        this.hours = hours;
        this.daysOfMonth = daysOfMonth;
        this.months = months;
        this.daysOfWeek = daysOfWeek;
        this.dayOfMonthWildcard = dayOfMonthWildcard;
        this.dayOfWeekWildcard = dayOfWeekWildcard;
    }

    /** @throws IllegalArgumentException if the expression is malformed */
    public static CronExpression parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Cron expression must not be empty");
        }
        String trimmed = expression.trim();
        String expanded = MACROS.getOrDefault(trimmed.toLowerCase(Locale.ROOT), trimmed);
        String[] fields = expanded.split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "Cron expression must have 5 fields (minute hour day-of-month month day-of-week): " + expression);
        }
        BitSet dow = parseField(fields[4], 0, 7, DAYS, 0, "day-of-week");
        if (dow.get(7)) { // 7 is an alias for Sunday
            dow.set(0);
            dow.clear(7);
        }
        return new CronExpression(trimmed,
                parseField(fields[0], 0, 59, null, 0, "minute"),
                parseField(fields[1], 0, 23, null, 0, "hour"),
                parseField(fields[2], 1, 31, null, 0, "day-of-month"),
                parseField(fields[3], 1, 12, MONTHS, 1, "month"),
                dow,
                fields[2].startsWith("*"),
                fields[4].startsWith("*"));
    }

    public static boolean isValid(String expression) {
        try {
            parse(expression);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Returns the first matching time strictly after {@code from} (with second precision dropped).
     *
     * @throws IllegalStateException if no matching time exists within the search horizon (e.g. {@code 0 0 31 2 *})
     */
    public ZonedDateTime nextAfter(ZonedDateTime from) {
        ZonedDateTime t = from.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        ZonedDateTime limit = t.plusYears(SEARCH_LIMIT_YEARS);
        while (t.isBefore(limit)) {
            if (!months.get(t.getMonthValue())) {
                t = t.plusMonths(1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            } else if (!dayMatches(t)) {
                t = t.plusDays(1).truncatedTo(ChronoUnit.DAYS);
            } else if (!hours.get(t.getHour())) {
                t = t.plusHours(1).truncatedTo(ChronoUnit.HOURS);
            } else if (!minutes.get(t.getMinute())) {
                t = t.plusMinutes(1);
            } else {
                return t;
            }
        }
        throw new IllegalStateException("Cron expression '" + expression + "' never matches");
    }

    public java.time.Instant nextAfter(java.time.Instant from) {
        return nextAfter(from.atZone(ZoneOffset.UTC)).toInstant();
    }

    public boolean matches(ZonedDateTime time) {
        return months.get(time.getMonthValue()) && dayMatches(time)
                && hours.get(time.getHour()) && minutes.get(time.getMinute());
    }

    private boolean dayMatches(ZonedDateTime t) {
        boolean domMatch = daysOfMonth.get(t.getDayOfMonth());
        boolean dowMatch = daysOfWeek.get(t.getDayOfWeek().getValue() % 7); // Monday=1 .. Sunday=7 -> 0
        if (!dayOfMonthWildcard && !dayOfWeekWildcard) {
            return domMatch || dowMatch;
        }
        return domMatch && dowMatch;
    }

    @Override
    public String toString() {
        return expression;
    }

    private static BitSet parseField(String field, int min, int max, List<String> names, int nameOffset, String label) {
        BitSet result = new BitSet(max + 1);
        for (String part : field.split(",", -1)) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("Empty list item in " + label + " field: '" + field + "'");
            }
            String rangePart = part;
            int step = 1;
            boolean hasStep = false;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                rangePart = part.substring(0, slash);
                step = parseNumber(part.substring(slash + 1), null, 0, label);
                hasStep = true;
                if (step < 1) {
                    throw new IllegalArgumentException("Step must be at least 1 in " + label + " field: '" + part + "'");
                }
            }
            int from;
            int to;
            if (rangePart.equals("*")) {
                from = min;
                to = max == 7 ? 6 : max; // "*" in day-of-week should not include the alias 7
            } else if (rangePart.contains("-")) {
                String[] bounds = rangePart.split("-", -1);
                if (bounds.length != 2) {
                    throw new IllegalArgumentException("Invalid range in " + label + " field: '" + part + "'");
                }
                from = parseNumber(bounds[0], names, nameOffset, label);
                to = parseNumber(bounds[1], names, nameOffset, label);
            } else {
                from = parseNumber(rangePart, names, nameOffset, label);
                to = hasStep ? max : from;
            }
            if (from < min || to > max || from > to) {
                throw new IllegalArgumentException(
                        "Value out of range " + min + "-" + max + " in " + label + " field: '" + part + "'");
            }
            for (int v = from; v <= to; v += step) {
                result.set(v);
            }
        }
        return result;
    }

    private static int parseNumber(String text, List<String> names, int nameOffset, String label) {
        if (names != null) {
            int idx = names.indexOf(text.toUpperCase(Locale.ROOT));
            if (idx >= 0) {
                return idx + nameOffset;
            }
        }
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value '" + text + "' in " + label + " field");
        }
    }
}
