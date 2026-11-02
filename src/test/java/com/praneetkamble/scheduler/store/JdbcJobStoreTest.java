package com.praneetkamble.scheduler.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.JobStatus;
import com.praneetkamble.scheduler.model.NewJob;
import com.praneetkamble.scheduler.support.TestDatabase;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcJobStoreTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(5);

    private HikariDataSource ds;
    private JdbcJobStore store;

    @BeforeEach
    void setUp() {
        ds = TestDatabase.create();
        store = new JdbcJobStore(ds);
    }

    @AfterEach
    void tearDown() {
        ds.close();
    }

    private Job insert(String type, Instant runAt) {
        return store.insert(new NewJob(type, "{\"k\":1}", null, 3, null), runAt, T0);
    }

    @Test
    void insertAndFindRoundTrip() {
        Job job = store.insert(new NewJob("log", "{\"message\":\"hi\"}", null, 7, "*/5 * * * *"), T0.plusSeconds(60), T0);

        Job found = store.find(job.id()).orElseThrow();
        assertThat(found).isEqualTo(job);
        assertThat(found.status()).isEqualTo(JobStatus.PENDING);
        assertThat(found.type()).isEqualTo("log");
        assertThat(found.payload()).isEqualTo("{\"message\":\"hi\"}");
        assertThat(found.runAt()).isEqualTo(T0.plusSeconds(60));
        assertThat(found.maxAttempts()).isEqualTo(7);
        assertThat(found.cron()).isEqualTo("*/5 * * * *");
        assertThat(found.attempts()).isZero();
        assertThat(found.isRecurring()).isTrue();
        assertThat(store.find("missing")).isEmpty();
    }

    @Test
    void claimOnlyReturnsDueJobsAndMarksThemRunning() {
        Job due = insert("a", T0);
        Job future = insert("b", T0.plusSeconds(10));

        List<Job> claimed = store.claimDue("w1", T0, LEASE, 10);

        assertThat(claimed).extracting(Job::id).containsExactly(due.id());
        Job running = claimed.get(0);
        assertThat(running.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(running.attempts()).isEqualTo(1);
        assertThat(running.lockedBy()).isEqualTo("w1");
        assertThat(running.lockedUntil()).isEqualTo(T0.plus(LEASE));
        assertThat(store.find(future.id()).orElseThrow().status()).isEqualTo(JobStatus.PENDING);
    }

    @Test
    void claimHonoursLimitAndRunAtOrder() {
        Job later = insert("later", T0.minusSeconds(10));
        Job earliest = insert("earliest", T0.minusSeconds(100));
        insert("latest", T0.minusSeconds(1));

        List<Job> claimed = store.claimDue("w1", T0, LEASE, 2);

        assertThat(claimed).extracting(Job::id).containsExactly(earliest.id(), later.id());
    }

    @Test
    void aClaimedJobCannotBeClaimedAgain() {
        insert("a", T0);
        assertThat(store.claimDue("w1", T0, LEASE, 5)).hasSize(1);
        assertThat(store.claimDue("w2", T0, LEASE, 5)).isEmpty();
    }

    @Test
    void concurrentClaimersNeverReceiveTheSameJob() throws Exception {
        int jobs = 60;
        for (int i = 0; i < jobs; i++) {
            insert("job" + i, T0);
        }
        int claimers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(claimers);
        try {
            List<Callable<List<Job>>> tasks = new ArrayList<>();
            for (int c = 0; c < claimers; c++) {
                String worker = "w" + c;
                tasks.add(() -> {
                    List<Job> mine = new ArrayList<>();
                    List<Job> batch;
                    while (!(batch = store.claimDue(worker, T0, LEASE, 5)).isEmpty()) {
                        mine.addAll(batch);
                    }
                    return mine;
                });
            }
            Set<String> seen = new HashSet<>();
            int total = 0;
            for (Future<List<Job>> f : pool.invokeAll(tasks)) {
                for (Job j : f.get()) {
                    total++;
                    assertThat(seen.add(j.id())).as("job %s claimed twice", j.id()).isTrue();
                }
            }
            assertThat(total).isEqualTo(jobs);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void successRequiresOwnership() {
        Job job = insert("a", T0);
        store.claimDue("w1", T0, LEASE, 1);

        assertThat(store.markSucceeded(job.id(), "intruder", T0)).isFalse();
        assertThat(store.find(job.id()).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);

        assertThat(store.markSucceeded(job.id(), "w1", T0.plusSeconds(1))).isTrue();
        Job done = store.find(job.id()).orElseThrow();
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.lockedBy()).isNull();
        assertThat(done.runCount()).isEqualTo(1);
        assertThat(store.markSucceeded(job.id(), "w1", T0)).as("second completion is a no-op").isFalse();
    }

    @Test
    void retryReturnsJobToPendingWithErrorAndNewRunAt() {
        Job job = insert("a", T0);
        store.claimDue("w1", T0, LEASE, 1);

        assertThat(store.scheduleRetry(job.id(), "w1", T0.plusSeconds(30), "boom", T0)).isTrue();

        Job retried = store.find(job.id()).orElseThrow();
        assertThat(retried.status()).isEqualTo(JobStatus.PENDING);
        assertThat(retried.runAt()).isEqualTo(T0.plusSeconds(30));
        assertThat(retried.lastError()).isEqualTo("boom");
        assertThat(retried.attempts()).as("attempt counter survives the retry").isEqualTo(1);
        assertThat(store.claimDue("w1", T0.plusSeconds(29), LEASE, 1)).isEmpty();
        assertThat(store.claimDue("w1", T0.plusSeconds(30), LEASE, 1)).singleElement()
                .satisfies(j -> assertThat(j.attempts()).isEqualTo(2));
    }

    @Test
    void deadJobsAreNotClaimed() {
        Job job = insert("a", T0);
        store.claimDue("w1", T0, LEASE, 1);
        assertThat(store.markDead(job.id(), "w1", "fatal", T0)).isTrue();

        assertThat(store.find(job.id()).orElseThrow().status()).isEqualTo(JobStatus.DEAD);
        assertThat(store.claimDue("w1", T0.plusSeconds(1000), LEASE, 1)).isEmpty();
    }

    @Test
    void recurringJobResetsAttemptsAndCountsRuns() {
        Job job = store.insert(new NewJob("a", "{}", null, 3, "* * * * *"), T0, T0);
        store.claimDue("w1", T0, LEASE, 1);

        assertThat(store.scheduleNextOccurrence(job.id(), "w1", T0.plusSeconds(60), null, T0)).isTrue();

        Job next = store.find(job.id()).orElseThrow();
        assertThat(next.status()).isEqualTo(JobStatus.PENDING);
        assertThat(next.attempts()).isZero();
        assertThat(next.runCount()).isEqualTo(1);
        assertThat(next.runAt()).isEqualTo(T0.plusSeconds(60));
    }

    @Test
    void cancelOnlyAffectsPendingJobs() {
        Job pending = insert("a", T0.plusSeconds(100));
        Job running = insert("b", T0);
        store.claimDue("w1", T0, LEASE, 1);

        assertThat(store.cancel(pending.id(), T0)).isTrue();
        assertThat(store.find(pending.id()).orElseThrow().status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(store.cancel(running.id(), T0)).isFalse();
        assertThat(store.cancel(pending.id(), T0)).as("already cancelled").isFalse();
        assertThat(store.claimDue("w2", T0.plusSeconds(500), LEASE, 5))
                .as("cancelled jobs are never claimed; running jobs are only reclaimed via lease recovery").isEmpty();
    }

    @Test
    void requeueRevivesDeadAndCancelledJobs() {
        Job dead = insert("a", T0);
        store.claimDue("w1", T0, LEASE, 1);
        store.markDead(dead.id(), "w1", "x", T0);
        Job cancelled = insert("b", T0.plusSeconds(100));
        store.cancel(cancelled.id(), T0);

        assertThat(store.requeue(dead.id(), T0.plusSeconds(5))).isTrue();
        assertThat(store.requeue(cancelled.id(), T0.plusSeconds(5))).isTrue();

        Job revived = store.find(dead.id()).orElseThrow();
        assertThat(revived.status()).isEqualTo(JobStatus.PENDING);
        assertThat(revived.attempts()).isZero();
        assertThat(revived.runAt()).isEqualTo(T0.plusSeconds(5));
        assertThat(store.requeue(revived.id(), T0)).as("pending jobs cannot be requeued").isFalse();
    }

    @Test
    void expiredLeasesAreRecovered() {
        Job retryable = insert("a", T0);
        Job exhausted = store.insert(new NewJob("b", "{}", null, 1, null), T0, T0);
        store.claimDue("crashed", T0, LEASE, 10);

        assertThat(store.recoverExpiredLeases(T0.plus(LEASE).minusSeconds(1))).as("lease still valid").isZero();
        assertThat(store.recoverExpiredLeases(T0.plus(LEASE).plusSeconds(1))).isEqualTo(2);

        Job back = store.find(retryable.id()).orElseThrow();
        assertThat(back.status()).isEqualTo(JobStatus.PENDING);
        assertThat(back.lockedBy()).isNull();
        assertThat(back.lastError()).contains("lease");
        Job dead = store.find(exhausted.id()).orElseThrow();
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
    }

    @Test
    void zombieWorkerCannotOverwriteARecoveredJob() {
        Job job = insert("a", T0);
        store.claimDue("zombie", T0, LEASE, 1);
        store.recoverExpiredLeases(T0.plus(LEASE).plusSeconds(1));
        store.claimDue("healthy", T0.plus(LEASE).plusSeconds(2), LEASE, 1);

        assertThat(store.markSucceeded(job.id(), "zombie", T0.plus(LEASE).plusSeconds(3))).isFalse();
        assertThat(store.find(job.id()).orElseThrow().lockedBy()).isEqualTo("healthy");
    }

    @Test
    void listAndCountByStatus() {
        insert("a", T0);
        insert("b", T0);
        Job c = insert("c", T0.plusSeconds(100));
        store.cancel(c.id(), T0);
        store.claimDue("w1", T0, LEASE, 1);

        assertThat(store.countByStatus()).containsEntry(JobStatus.RUNNING, 1L).containsEntry(JobStatus.PENDING, 1L)
                .containsEntry(JobStatus.CANCELLED, 1L);
        assertThat(store.list(null, 10)).hasSize(3);
        assertThat(store.list(JobStatus.CANCELLED, 10)).extracting(Job::id).containsExactly(c.id());
        assertThat(store.list(null, 2)).hasSize(2);
    }
}
