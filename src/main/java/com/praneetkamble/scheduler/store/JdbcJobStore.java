package com.praneetkamble.scheduler.store;

import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.JobStatus;
import com.praneetkamble.scheduler.model.NewJob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Plain-JDBC {@link JobStore} that sticks to portable SQL (timestamps are epoch milliseconds), so it runs
 * unchanged on H2 and PostgreSQL.
 *
 * <p>Claiming is optimistic rather than lock-based: candidates are selected, then each one is taken with an
 * {@code UPDATE ... WHERE status = 'PENDING'} and only counts if exactly one row changed. Competing workers
 * therefore never execute the same job twice, without needing {@code SKIP LOCKED}.
 */
public class JdbcJobStore implements JobStore {

    private static final String COLUMNS = "id, type, payload, status, run_at, attempts, max_attempts, cron, run_count, "
            + "last_error, locked_by, locked_until, created_at, updated_at";
    private static final int MAX_ERROR_LENGTH = 2000;

    private final DataSource dataSource;

    public JdbcJobStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Job insert(NewJob job, Instant runAt, Instant now) {
        String id = UUID.randomUUID().toString();
        String sql = "INSERT INTO jobs (id, type, payload, status, run_at, attempts, max_attempts, cron, run_count, "
                + "created_at, updated_at) VALUES (?, ?, ?, 'PENDING', ?, 0, ?, ?, 0, ?, ?)";
        update(sql, id, job.type(), job.payload(), runAt.toEpochMilli(), job.maxAttempts(), job.cron(),
                now.toEpochMilli(), now.toEpochMilli());
        return find(id).orElseThrow();
    }

    @Override
    public Optional<Job> find(String id) {
        List<Job> result = query("SELECT " + COLUMNS + " FROM jobs WHERE id = ?", id);
        return result.stream().findFirst();
    }

    @Override
    public List<Job> list(JobStatus status, int limit) {
        if (status == null) {
            return query("SELECT " + COLUMNS + " FROM jobs ORDER BY created_at DESC, id LIMIT ?", limit);
        }
        return query("SELECT " + COLUMNS + " FROM jobs WHERE status = ? ORDER BY created_at DESC, id LIMIT ?",
                status.name(), limit);
    }

    @Override
    public Map<JobStatus, Long> countByStatus() {
        Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
        for (JobStatus s : JobStatus.values()) {
            counts.put(s, 0L);
        }
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT status, COUNT(*) FROM jobs GROUP BY status");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                counts.put(JobStatus.valueOf(rs.getString(1)), rs.getLong(2));
            }
        } catch (SQLException e) {
            throw new StoreException("Failed to count jobs", e);
        }
        return counts;
    }

    @Override
    public List<Job> claimDue(String workerId, Instant now, Duration lease, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<String> candidates = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id FROM jobs WHERE status = 'PENDING' AND run_at <= ? ORDER BY run_at, created_at LIMIT ?")) {
            ps.setLong(1, now.toEpochMilli());
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    candidates.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new StoreException("Failed to select due jobs", e);
        }

        List<Job> claimed = new ArrayList<>();
        for (String id : candidates) {
            int changed = update("UPDATE jobs SET status = 'RUNNING', locked_by = ?, locked_until = ?, "
                            + "attempts = attempts + 1, updated_at = ? WHERE id = ? AND status = 'PENDING'",
                    workerId, now.plus(lease).toEpochMilli(), now.toEpochMilli(), id);
            if (changed == 1) {
                find(id).ifPresent(claimed::add);
            }
        }
        return claimed;
    }

    @Override
    public boolean markSucceeded(String id, String workerId, Instant now) {
        return update("UPDATE jobs SET status = 'SUCCEEDED', run_count = run_count + 1, last_error = NULL, "
                        + "locked_by = NULL, locked_until = NULL, updated_at = ? "
                        + "WHERE id = ? AND status = 'RUNNING' AND locked_by = ?",
                now.toEpochMilli(), id, workerId) == 1;
    }

    @Override
    public boolean scheduleRetry(String id, String workerId, Instant runAt, String error, Instant now) {
        return update("UPDATE jobs SET status = 'PENDING', run_at = ?, last_error = ?, locked_by = NULL, "
                        + "locked_until = NULL, updated_at = ? WHERE id = ? AND status = 'RUNNING' AND locked_by = ?",
                runAt.toEpochMilli(), truncate(error), now.toEpochMilli(), id, workerId) == 1;
    }

    @Override
    public boolean markDead(String id, String workerId, String error, Instant now) {
        return update("UPDATE jobs SET status = 'DEAD', last_error = ?, locked_by = NULL, locked_until = NULL, "
                        + "updated_at = ? WHERE id = ? AND status = 'RUNNING' AND locked_by = ?",
                truncate(error), now.toEpochMilli(), id, workerId) == 1;
    }

    @Override
    public boolean scheduleNextOccurrence(String id, String workerId, Instant nextRunAt, String error, Instant now) {
        return update("UPDATE jobs SET status = 'PENDING', run_at = ?, attempts = 0, run_count = run_count + 1, "
                        + "last_error = ?, locked_by = NULL, locked_until = NULL, updated_at = ? "
                        + "WHERE id = ? AND status = 'RUNNING' AND locked_by = ?",
                nextRunAt.toEpochMilli(), truncate(error), now.toEpochMilli(), id, workerId) == 1;
    }

    @Override
    public boolean cancel(String id, Instant now) {
        return update("UPDATE jobs SET status = 'CANCELLED', updated_at = ? WHERE id = ? AND status = 'PENDING'",
                now.toEpochMilli(), id) == 1;
    }

    @Override
    public boolean requeue(String id, Instant now) {
        return update("UPDATE jobs SET status = 'PENDING', run_at = ?, attempts = 0, updated_at = ? "
                        + "WHERE id = ? AND status IN ('DEAD', 'CANCELLED')",
                now.toEpochMilli(), now.toEpochMilli(), id) == 1;
    }

    @Override
    public int recoverExpiredLeases(Instant now) {
        long nowMs = now.toEpochMilli();
        int retried = update("UPDATE jobs SET status = 'PENDING', run_at = ?, last_error = 'Worker lease expired', "
                        + "locked_by = NULL, locked_until = NULL, updated_at = ? "
                        + "WHERE status = 'RUNNING' AND locked_until < ? AND attempts < max_attempts",
                nowMs, nowMs, nowMs);
        int dead = update("UPDATE jobs SET status = 'DEAD', last_error = 'Worker lease expired', "
                        + "locked_by = NULL, locked_until = NULL, updated_at = ? "
                        + "WHERE status = 'RUNNING' AND locked_until < ? AND attempts >= max_attempts",
                nowMs, nowMs);
        return retried + dead;
    }

    private static String truncate(String error) {
        if (error == null || error.length() <= MAX_ERROR_LENGTH) {
            return error;
        }
        return error.substring(0, MAX_ERROR_LENGTH);
    }

    private List<Job> query(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                List<Job> jobs = new ArrayList<>();
                while (rs.next()) {
                    jobs.add(map(rs));
                }
                return jobs;
            }
        } catch (SQLException e) {
            throw new StoreException("Query failed: " + sql, e);
        }
    }

    private int update(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("Update failed: " + sql, e);
        }
    }

    private static void bind(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object p = params[i];
            if (p == null) {
                ps.setNull(i + 1, Types.VARCHAR);
            } else if (p instanceof Long l) {
                ps.setLong(i + 1, l);
            } else if (p instanceof Integer n) {
                ps.setInt(i + 1, n);
            } else {
                ps.setString(i + 1, p.toString());
            }
        }
    }

    private static Job map(ResultSet rs) throws SQLException {
        long lockedUntil = rs.getLong("locked_until");
        boolean noLease = rs.wasNull();
        return new Job(
                rs.getString("id"),
                rs.getString("type"),
                rs.getString("payload"),
                JobStatus.valueOf(rs.getString("status")),
                Instant.ofEpochMilli(rs.getLong("run_at")),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                rs.getString("cron"),
                rs.getLong("run_count"),
                rs.getString("last_error"),
                rs.getString("locked_by"),
                noLease ? null : Instant.ofEpochMilli(lockedUntil),
                Instant.ofEpochMilli(rs.getLong("created_at")),
                Instant.ofEpochMilli(rs.getLong("updated_at")));
    }
}
