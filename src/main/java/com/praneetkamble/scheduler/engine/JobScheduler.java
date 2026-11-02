package com.praneetkamble.scheduler.engine;

import com.praneetkamble.scheduler.cron.CronExpression;
import com.praneetkamble.scheduler.handler.HandlerRegistry;
import com.praneetkamble.scheduler.handler.JobContext;
import com.praneetkamble.scheduler.handler.JobHandler;
import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.NewJob;
import com.praneetkamble.scheduler.store.JobStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls the {@link JobStore} for due jobs and runs them on a bounded worker pool.
 *
 * <p>Life cycle of a job: {@code PENDING} -> claimed ({@code RUNNING}) -> {@code SUCCEEDED}; on failure it
 * returns to {@code PENDING} with an exponentially growing delay until {@code maxAttempts} is reached, then
 * {@code DEAD}. Recurring (cron) jobs always return to {@code PENDING} for their next occurrence.
 *
 * <p>A semaphore with one permit per worker caps the number of claims, so the scheduler never takes more work
 * from the database than it can execute - other instances can pick up the rest.
 */
public class JobScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final JobStore store;
    private final HandlerRegistry handlers;
    private final SchedulerConfig config;
    private final Clock clock;
    private final Semaphore capacity;
    private final ExecutorService workers;
    private final ScheduledExecutorService poller;
    private final AtomicLong executed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile boolean started;

    public JobScheduler(JobStore store, HandlerRegistry handlers, SchedulerConfig config, Clock clock) {
        this.store = store;
        this.handlers = handlers;
        this.config = config;
        this.clock = clock;
        this.capacity = new Semaphore(config.workers());
        this.workers = Executors.newFixedThreadPool(config.workers(), namedFactory("job-worker-"));
        this.poller = Executors.newSingleThreadScheduledExecutor(namedFactory("job-poller-"));
    }

    /** Enqueues a job. Recurring jobs start at their next cron occurrence unless {@code runAt} is given. */
    public Job submit(NewJob request) {
        Instant now = clock.instant();
        Instant runAt = request.runAt();
        if (runAt == null) {
            runAt = request.cron() == null ? now : CronExpression.parse(request.cron()).nextAfter(now);
        } else if (request.cron() != null) {
            CronExpression.parse(request.cron()); // validate eagerly
        }
        return store.insert(request, runAt, now);
    }

    public JobStore store() {
        return store;
    }

    public String workerId() {
        return config.workerId();
    }

    public long executedCount() {
        return executed.get();
    }

    public long failedAttempts() {
        return failed.get();
    }

    public int busyWorkers() {
        return config.workers() - capacity.availablePermits();
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        poller.scheduleWithFixedDelay(this::pollSafely, 0, config.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
        log.info("Scheduler {} started with {} workers", config.workerId(), config.workers());
    }

    private void pollSafely() {
        try {
            pollOnce();
        } catch (RuntimeException e) {
            log.error("Polling failed", e); // keep the poller alive (e.g. transient database outage)
        }
    }

    /** One polling cycle; package-visible so tests can drive the scheduler deterministically. */
    void pollOnce() {
        Instant now = clock.instant();
        int recovered = store.recoverExpiredLeases(now);
        if (recovered > 0) {
            log.warn("Recovered {} job(s) whose worker lease expired", recovered);
        }
        int free = capacity.drainPermits();
        if (free == 0) {
            return;
        }
        List<Job> claimed;
        try {
            claimed = store.claimDue(config.workerId(), now, config.lease(), free);
        } catch (RuntimeException e) {
            capacity.release(free);
            throw e;
        }
        capacity.release(free - claimed.size());
        for (Job job : claimed) {
            workers.execute(() -> run(job));
        }
    }

    private void run(Job job) {
        try {
            JobHandler handler = handlers.find(job.type()).orElse(null);
            if (handler == null) {
                onFailure(job, new IllegalStateException("No handler registered for job type '" + job.type() + "'"));
                return;
            }
            try {
                handler.handle(new JobContext(job));
            } catch (Exception e) {
                onFailure(job, e);
                return;
            }
            onSuccess(job);
        } catch (RuntimeException e) {
            log.error("Could not record outcome of job {}", job.id(), e);
        } finally {
            capacity.release();
        }
    }

    private void onSuccess(Job job) {
        executed.incrementAndGet();
        Instant now = clock.instant();
        boolean recorded = job.isRecurring()
                ? store.scheduleNextOccurrence(job.id(), config.workerId(), nextOccurrence(job, now), null, now)
                : store.markSucceeded(job.id(), config.workerId(), now);
        warnIfLeaseLost(job, recorded);
        log.debug("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempts());
    }

    private void onFailure(Job job, Throwable error) {
        failed.incrementAndGet();
        Instant now = clock.instant();
        String message = describe(error);
        boolean recorded;
        if (job.attempts() < job.maxAttempts()) {
            Duration delay = config.backoff().delayAfter(job.attempts());
            log.warn("Job {} ({}) failed on attempt {}/{}: {}. Retrying in {} ms", job.id(), job.type(),
                    job.attempts(), job.maxAttempts(), message, delay.toMillis());
            recorded = store.scheduleRetry(job.id(), config.workerId(), now.plus(delay), message, now);
        } else if (job.isRecurring()) {
            log.error("Recurring job {} ({}) exhausted its attempts: {}. Waiting for next occurrence", job.id(),
                    job.type(), message);
            recorded = store.scheduleNextOccurrence(job.id(), config.workerId(), nextOccurrence(job, now), message, now);
        } else {
            log.error("Job {} ({}) is dead after {} attempts: {}", job.id(), job.type(), job.attempts(), message);
            recorded = store.markDead(job.id(), config.workerId(), message, now);
        }
        warnIfLeaseLost(job, recorded);
    }

    private static Instant nextOccurrence(Job job, Instant now) {
        return CronExpression.parse(job.cron()).nextAfter(now);
    }

    private static void warnIfLeaseLost(Job job, boolean recorded) {
        if (!recorded) {
            log.warn("Job {} was no longer owned by this worker when its outcome was recorded (lease expired?)", job.id());
        }
    }

    private static String describe(Throwable t) {
        return t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
    }

    /** Stops polling and waits up to {@code timeout} for running jobs to finish. */
    public void shutdown(Duration timeout) {
        poller.shutdown();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Workers still busy after {} ms, interrupting", timeout.toMillis());
                workers.shutdownNow();
            }
            poller.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        shutdown(Duration.ofSeconds(10));
    }

    private static java.util.concurrent.ThreadFactory namedFactory(String prefix) {
        AtomicLong n = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
