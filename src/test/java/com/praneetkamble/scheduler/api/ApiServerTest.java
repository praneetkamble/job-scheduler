package com.praneetkamble.scheduler.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.praneetkamble.scheduler.engine.BackoffPolicy;
import com.praneetkamble.scheduler.engine.JobScheduler;
import com.praneetkamble.scheduler.engine.SchedulerConfig;
import com.praneetkamble.scheduler.handler.HandlerRegistry;
import com.praneetkamble.scheduler.store.JdbcJobStore;
import com.praneetkamble.scheduler.support.TestDatabase;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** End-to-end: real HTTP server, real scheduler threads, in-memory H2. */
class ApiServerTest {

    private HikariDataSource ds;
    private JobScheduler scheduler;
    private ApiServer api;
    private final HttpClient http = HttpClient.newHttpClient();
    private String base;

    @BeforeEach
    void setUp() throws Exception {
        ds = TestDatabase.create();
        HandlerRegistry registry = HandlerRegistry.withBuiltIns();
        Clock clock = Clock.systemUTC();
        scheduler = new JobScheduler(new JdbcJobStore(ds), registry,
                new SchedulerConfig("api-test", 2, Duration.ofMillis(50), Duration.ofMinutes(5),
                        new BackoffPolicy(Duration.ofMillis(50), Duration.ofMillis(200), 0, () -> 0)), clock);
        api = new ApiServer(scheduler, registry.types(), clock, 0);
        scheduler.start();
        api.start();
        base = "http://localhost:" + api.port();
    }

    @AfterEach
    void tearDown() {
        api.close();
        scheduler.shutdown(Duration.ofSeconds(2));
        ds.close();
    }

    private HttpResponse<String> call(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path));
        if (body == null) {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            b.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> r) throws Exception {
        return Json.MAPPER.readTree(r.body());
    }

    private String awaitStatus(String id, String status) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(json(call("GET", "/api/jobs/" + id, null)).get("status").asText()).isEqualTo(status));
        return status;
    }

    @Test
    void createdJobIsExecutedAndQueryable() throws Exception {
        HttpResponse<String> created = call("POST", "/api/jobs",
                "{\"type\":\"log\",\"payload\":{\"message\":\"hi\"}}");

        assertThat(created.statusCode()).isEqualTo(201);
        JsonNode job = json(created);
        String id = job.get("id").asText();
        assertThat(created.headers().firstValue("Location")).contains("/api/jobs/" + id);
        assertThat(job.get("payload").get("message").asText()).isEqualTo("hi");

        awaitStatus(id, "SUCCEEDED");
        assertThat(json(call("GET", "/api/jobs?status=succeeded", null))).hasSize(1);
        assertThat(json(call("GET", "/api/jobs", null)).get(0).get("id").asText()).isEqualTo(id);
    }

    @Test
    void flakyJobRetriesThroughTheApiAndFinallySucceeds() throws Exception {
        JsonNode job = json(call("POST", "/api/jobs",
                "{\"type\":\"flaky\",\"payload\":{\"failTimes\":2},\"maxAttempts\":4}"));
        String id = job.get("id").asText();

        awaitStatus(id, "SUCCEEDED");
        assertThat(json(call("GET", "/api/jobs/" + id, null)).get("attempts").asInt()).isEqualTo(3);
    }

    @Test
    void delayedJobCanBeCancelledAndThenRetried() throws Exception {
        JsonNode job = json(call("POST", "/api/jobs", "{\"type\":\"log\",\"delaySeconds\":3600}"));
        String id = job.get("id").asText();
        assertThat(job.get("status").asText()).isEqualTo("PENDING");

        HttpResponse<String> cancelled = call("DELETE", "/api/jobs/" + id, null);
        assertThat(cancelled.statusCode()).isEqualTo(200);
        assertThat(json(cancelled).get("status").asText()).isEqualTo("CANCELLED");
        assertThat(call("DELETE", "/api/jobs/" + id, null).statusCode()).as("cannot cancel twice").isEqualTo(409);

        HttpResponse<String> retried = call("POST", "/api/jobs/" + id + "/retry", null);
        assertThat(retried.statusCode()).isEqualTo(200);
        awaitStatus(id, "SUCCEEDED");
        assertThat(call("POST", "/api/jobs/" + id + "/retry", null).statusCode()).isEqualTo(409);
    }

    @Test
    void cronJobsAreAcceptedAndScheduledInTheFuture() throws Exception {
        JsonNode job = json(call("POST", "/api/jobs", "{\"type\":\"log\",\"cron\":\"0 0 1 1 *\"}"));
        assertThat(job.get("cron").asText()).isEqualTo("0 0 1 1 *");
        assertThat(job.get("status").asText()).isEqualTo("PENDING");
    }

    @Test
    void validationErrorsAreReportedAsBadRequests() throws Exception {
        assertThat(call("POST", "/api/jobs", "{}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{\"type\":\"nope\"}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{\"type\":\"log\",\"cron\":\"bad\"}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{\"type\":\"log\",\"maxAttempts\":0}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{\"type\":\"log\",\"runAt\":\"tomorrow\"}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{\"type\":\"log\",\"payload\":[1]}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/jobs", "{broken").statusCode()).isEqualTo(400);
        assertThat(call("GET", "/api/jobs?status=wat", null).statusCode()).isEqualTo(400);
        assertThat(json(call("POST", "/api/jobs", "{}")).get("error").asText()).contains("type");
    }

    @Test
    void unknownRoutesAndMethods() throws Exception {
        assertThat(call("GET", "/api/jobs/does-not-exist", null).statusCode()).isEqualTo(404);
        assertThat(call("DELETE", "/api/jobs/does-not-exist", null).statusCode()).isEqualTo(404);
        assertThat(call("GET", "/nothing", null).statusCode()).isEqualTo(404);
        assertThat(call("PUT", "/api/jobs", "{}").statusCode()).isEqualTo(405);
    }

    @Test
    void statsHealthAndDashboard() throws Exception {
        JsonNode job = json(call("POST", "/api/jobs", "{\"type\":\"log\"}"));
        awaitStatus(job.get("id").asText(), "SUCCEEDED");

        JsonNode stats = json(call("GET", "/api/stats", null));
        assertThat(stats.get("counts").get("SUCCEEDED").asInt()).isEqualTo(1);
        assertThat(stats.get("total").asInt()).isEqualTo(1);
        assertThat(stats.get("jobTypes")).extracting(JsonNode::asText).contains("log", "sleep", "flaky", "http");

        assertThat(json(call("GET", "/health", null)).get("status").asText()).isEqualTo("UP");

        HttpResponse<String> page = call("GET", "/", null);
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).startsWith("text/html"));
        assertThat(page.body()).contains("Job Scheduler");
    }
}
