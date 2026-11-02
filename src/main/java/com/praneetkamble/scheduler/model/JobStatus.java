package com.praneetkamble.scheduler.model;

public enum JobStatus {
    /** Waiting for its run time (also used while waiting to retry). */
    PENDING,
    /** Claimed by a worker and currently executing. */
    RUNNING,
    SUCCEEDED,
    /** Exhausted all attempts. Can be re-queued manually. */
    DEAD,
    CANCELLED
}
