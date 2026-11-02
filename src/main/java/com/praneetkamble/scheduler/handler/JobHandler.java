package com.praneetkamble.scheduler.handler;

/**
 * Business logic for one job type. Throwing any exception marks the attempt as failed and triggers the
 * retry policy. Implementations should be idempotent: a job is delivered at least once.
 */
@FunctionalInterface
public interface JobHandler {

    void handle(JobContext context) throws Exception;
}
