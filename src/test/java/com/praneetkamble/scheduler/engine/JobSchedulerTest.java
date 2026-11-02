package com.praneetkamble.scheduler.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.praneetkamble.scheduler.handler.HandlerRegistry;
import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.JobStatus;
import com.praneetkamble.scheduler.model.NewJob;
import com.praneetkamble.scheduler.store.JdbcJobStore;
import com.praneetkamble.scheduler.support.MutableClock;
import com.praneetkamble.scheduler.support.TestDatabase;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Drives the scheduler deterministically: a mutable clock and manual polling instead of a running poller thread. */
class JobSchedulerTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private HikariDataSource ds;
    private JdbcJobStore store;
    private MutableClock clock;
    private HandlerRegistry registry;
    private JobScheduler scheduler;

    @BeforeEach
    void setUp() {
        ds = TestDatabase.create();
        store = new JdbcJobStore(ds);
        clock = new MutableClock(T0);
        registry = new HandlerRegistry();
        scheduler = newScheduler(2);
    }

    private JobScheduler newScheduler(int workers) {
        BackoffPolicy backoff = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofMinutes(10), 0, () -> 0);
        return new JobScheduler(store, registry,
                new SchedulerConfig("test-worker", workers, Duration.ofMillis(50), Duration.ofMinutes(5), backoff), clock);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown(Duration.ofSeconds(2));
        ds.close();
    }

    private Job awaitStatus(String id, JobStatus status) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                () -> assertThat(store.find(id).orElseThrow().status()).isEqualTo(status));
        return store.find(id).orElseThrow();
    }

    @Test
    void runsAnImmediateJobToSuccess() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("count", ctx -> calls.incrementAndGet());
        Job job = scheduler.submit(NewJob.of("count", "{}"));

        scheduler.pollOnce();

        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
        assertThat(calls).hasValue(1);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(scheduler.executedCount()).isEqualTo(1);
    }

    @Test
    void delayedJobWaitsUntilItIsDue() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("count", ctx -> calls.incrementAndGet());
        Job job = scheduler.submit(new NewJob("count", "{}", T0.plusSeconds(60), 3, null));

        scheduler.pollOnce();
        assertThat(store.find(job.id()).orElseThrow().status()).isEqualTo(JobStatus.PENDING);
        assertThat(calls).hasValue(0);

        clock.advance(Duration.ofSeconds(60));
        scheduler.pollOnce();
        awaitStatus(job.id(), JobStatus.SUCCEEDED);
        assertThat(calls).hasValue(1);
    }

    @Test
    void failedJobIsRetriedWithExponentialBackoffAndEventuallySucceeds() {
        registry.register("flaky", ctx -> {
            if (ctx.attempt() < 3) {
                throw new IllegalStateException("not yet " + ctx.attempt());
            }
        });
        Job job = scheduler.submit(new NewJob("flaky", "{}", null, 5, null));

        scheduler.pollOnce(); // attempt 1 fails -> retry in 10s
        Job afterFirst = awaitPendingWithAttempts(job.id(), 1);
        assertThat(afterFirst.runAt()).isEqualTo(T0.plusSeconds(10));
        assertThat(afterFirst.lastError()).contains("not yet 1");

        scheduler.pollOnce(); // not due yet
        assertThat(store.find(job.id()).orElseThrow().attempts()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(10));
        scheduler.pollOnce(); // attempt 2 fails -> retry in 20s
        Job afterSecond = awaitPendingWithAttempts(job.id(), 2);
        assertThat(afterSecond.runAt()).isEqualTo(T0.plusSeconds(30));

        clock.advance(Duration.ofSeconds(20));
        scheduler.pollOnce(); // attempt 3 succeeds
        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(3);
        assertThat(scheduler.failedAttempts()).isEqualTo(2);
    }

    private Job awaitPendingWithAttempts(String id, int attempts) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Job j = store.find(id).orElseThrow();
            assertThat(j.status()).isEqualTo(JobStatus.PENDING);
            assertThat(j.attempts()).isEqualTo(attempts);
            assertThat(j.lastError()).isNotNull();
        });
        return store.find(id).orElseThrow();
    }

    @Test
    void jobBecomesDeadAfterMaxAttempts() {
        registry.register("bad", ctx -> {
            throw new RuntimeException("always fails");
        });
        Job job = scheduler.submit(new NewJob("bad", "{}", null, 2, null));

        scheduler.pollOnce();
        awaitPendingWithAttempts(job.id(), 1);
        clock.advance(Duration.ofSeconds(10));
        scheduler.pollOnce();

        Job dead = awaitStatus(job.id(), JobStatus.DEAD);
        assertThat(dead.attempts()).isEqualTo(2);
        assertThat(dead.lastError()).contains("always fails");

        clock.advance(Duration.ofDays(1));
        scheduler.pollOnce();
        assertThat(store.find(job.id()).orElseThrow().attempts()).as("dead jobs stay dead").isEqualTo(2);
    }

    @Test
    void unknownJobTypeEndsUpDeadWithHelpfulError() {
        Job job = scheduler.submit(new NewJob("ghost", "{}", null, 1, null));
        scheduler.pollOnce();
        Job dead = awaitStatus(job.id(), JobStatus.DEAD);
        assertThat(dead.lastError()).contains("No handler registered").contains("ghost");
    }

    @Test
    void recurringJobIsRescheduledAfterEveryRun() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("tick", ctx -> calls.incrementAndGet());
        Job job = scheduler.submit(new NewJob("tick", "{}", null, 3, "*/5 * * * *"));
        assertThat(job.runAt()).as("first run at next cron occurrence").isEqualTo(Instant.parse("2026-01-01T00:05:00Z"));

        scheduler.pollOnce();
        assertThat(calls).hasValue(0);

        clock.advance(Duration.ofMinutes(5));
        scheduler.pollOnce();
        await().atMost(5, TimeUnit.SECONDS).until(() -> store.find(job.id()).orElseThrow().runCount() == 1);
        Job afterFirst = store.find(job.id()).orElseThrow();
        assertThat(afterFirst.status()).isEqualTo(JobStatus.PENDING);
        assertThat(afterFirst.runAt()).isEqualTo(Instant.parse("2026-01-01T00:10:00Z"));

        clock.advance(Duration.ofMinutes(5));
        scheduler.pollOnce();
        await().atMost(5, TimeUnit.SECONDS).until(() -> store.find(job.id()).orElseThrow().runCount() == 2);
        assertThat(calls).hasValue(2);
    }

    @Test
    void recurringJobSurvivesExhaustedRetries() {
        registry.register("bad", ctx -> {
            throw new RuntimeException("nope");
        });
        Job job = scheduler.submit(new NewJob("bad", "{}", T0, 1, "0 * * * *"));

        scheduler.pollOnce();

        await().atMost(5, TimeUnit.SECONDS).until(() -> store.find(job.id()).orElseThrow().runCount() == 1);
        Job j = store.find(job.id()).orElseThrow();
        assertThat(j.status()).as("recurring jobs are never DEAD").isEqualTo(JobStatus.PENDING);
        assertThat(j.lastError()).contains("nope");
        assertThat(j.runAt()).isEqualTo(Instant.parse("2026-01-01T01:00:00Z"));
    }

    @Test
    void neverRunsMoreJobsConcurrentlyThanWorkers() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        registry.register("block", ctx -> {
            maxSeen.accumulateAndGet(running.incrementAndGet(), Math::max);
            release.await(5, TimeUnit.SECONDS);
            running.decrementAndGet();
        });
        for (int i = 0; i < 5; i++) {
            scheduler.submit(NewJob.of("block", "{}"));
        }

        scheduler.pollOnce();
        await().atMost(5, TimeUnit.SECONDS).until(() -> running.get() == 2);
        scheduler.pollOnce(); // no free workers: must not claim anything more
        assertThat(store.countByStatus().get(JobStatus.RUNNING)).isEqualTo(2L);
        assertThat(store.countByStatus().get(JobStatus.PENDING)).isEqualTo(3L);
        assertThat(scheduler.busyWorkers()).isEqualTo(2);

        release.countDown();
        await().atMost(5, TimeUnit.SECONDS).until(() -> scheduler.busyWorkers() == 0);
        for (int i = 0; i < 3; i++) {
            scheduler.pollOnce();
            await().atMost(5, TimeUnit.SECONDS).until(() -> scheduler.busyWorkers() == 0);
        }
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> store.countByStatus().getOrDefault(JobStatus.SUCCEEDED, 0L) == 5L);
        assertThat(maxSeen).hasValueLessThanOrEqualTo(2);
    }

    @Test
    void jobsOfACrashedWorkerAreRecoveredAfterTheLeaseExpires() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("count", ctx -> calls.incrementAndGet());
        Job job = scheduler.submit(NewJob.of("count", "{}"));
        // another worker claims the job and then dies without reporting back
        store.claimDue("crashed-worker", T0, Duration.ofMinutes(5), 1);

        scheduler.pollOnce();
        assertThat(calls).hasValue(0);

        clock.advance(Duration.ofMinutes(6));
        scheduler.pollOnce();
        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);
        assertThat(done.attempts()).as("the crashed attempt counts").isEqualTo(2);
        assertThat(calls).hasValue(1);
    }

    @Test
    void submitValidatesCronEagerly() {
        assertThatThrownBy(() -> scheduler.submit(new NewJob("x", "{}", T0, 3, "not a cron")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void backgroundPollerPicksUpJobsOnItsOwn() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("count", ctx -> calls.incrementAndGet());
        scheduler.start();
        Job job = scheduler.submit(NewJob.of("count", "{}"));
        awaitStatus(job.id(), JobStatus.SUCCEEDED);
        assertThat(calls).hasValue(1);
    }

    @Test
    void duplicateHandlerRegistrationIsRejected() {
        registry.register("a", ctx -> { });
        assertThatThrownBy(() -> registry.register("a", ctx -> { })).isInstanceOf(IllegalStateException.class);
    }
}
