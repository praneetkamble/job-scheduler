package com.praneetkamble.scheduler.store;

import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.JobStatus;
import com.praneetkamble.scheduler.model.NewJob;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Persistence port of the scheduler. All state transitions are expressed as guarded, atomic updates so
 * that several scheduler processes can safely share one database.
 */
public interface JobStore {

    /** Inserts a new {@code PENDING} job that becomes due at {@code runAt}. */
    Job insert(NewJob job, Instant runAt, Instant now);

    Optional<Job> find(String id);

    /** Newest first. {@code status} may be {@code null} to list everything. */
    List<Job> list(JobStatus status, int limit);

    Map<JobStatus, Long> countByStatus();

    /**
     * Atomically claims up to {@code limit} due jobs for {@code workerId}. Claimed jobs move to
     * {@code RUNNING}, have their attempt counter incremented and hold a lease until {@code now + lease}.
     */
    List<Job> claimDue(String workerId, Instant now, Duration lease, int limit);

    /** Marks a non-recurring job as finished. Returns false if the caller no longer owns the job. */
    boolean markSucceeded(String id, String workerId, Instant now);

    /** Puts a failed job back to {@code PENDING} to run again at {@code runAt}. */
    boolean scheduleRetry(String id, String workerId, Instant runAt, String error, Instant now);

    /** Gives up on a job after its last attempt. */
    boolean markDead(String id, String workerId, String error, Instant now);

    /** Completes a run of a recurring job and queues its next occurrence. {@code error} may be null. */
    boolean scheduleNextOccurrence(String id, String workerId, Instant nextRunAt, String error, Instant now);

    /** Cancels a job that has not started yet. */
    boolean cancel(String id, Instant now);

    /** Re-queues a {@code DEAD} or {@code CANCELLED} job for an immediate fresh run. */
    boolean requeue(String id, Instant now);

    /**
     * Recovers jobs whose worker vanished (lease expired): they go back to {@code PENDING} if attempts remain,
     * otherwise to {@code DEAD}. Returns the number of jobs touched.
     */
    int recoverExpiredLeases(Instant now);
}
