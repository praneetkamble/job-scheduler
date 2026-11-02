package com.praneetkamble.scheduler.api;

import com.praneetkamble.scheduler.cron.CronExpression;
import com.praneetkamble.scheduler.engine.JobScheduler;
import com.praneetkamble.scheduler.model.Job;
import com.praneetkamble.scheduler.model.JobStatus;
import com.praneetkamble.scheduler.model.NewJob;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal REST API and dashboard on top of the JDK's built-in HTTP server (one virtual thread per request).
 *
 * <pre>
 * GET    /                      dashboard
 * GET    /health                liveness
 * GET    /api/stats             counters per status and scheduler metrics
 * GET    /api/jobs?status=&amp;limit=   list jobs
 * POST   /api/jobs              enqueue a job
 * GET    /api/jobs/{id}         job details
 * DELETE /api/jobs/{id}         cancel a pending job
 * POST   /api/jobs/{id}/retry   re-queue a dead or cancelled job
 * </pre>
 */
public class ApiServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ApiServer.class);
    private static final int MAX_BODY_BYTES = 1_000_000;

    private final JobScheduler scheduler;
    private final Set<String> knownTypes;
    private final Clock clock;
    private final HttpServer server;
    private final byte[] dashboard;

    public ApiServer(JobScheduler scheduler, Set<String> knownTypes, Clock clock, int port) throws IOException {
        this.scheduler = scheduler;
        this.knownTypes = new TreeSet<>(knownTypes);
        this.clock = clock;
        this.dashboard = loadDashboard();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/", this::dispatch);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public void start() {
        server.start();
        log.info("HTTP API listening on port {}", port());
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
    }

    private void dispatch(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (ApiException e) {
            send(ex, e.status(), Json.MAPPER.createObjectNode().put("error", e.getMessage()));
        } catch (RuntimeException e) {
            log.error("Unhandled error for {} {}", ex.getRequestMethod(), ex.getRequestURI(), e);
            send(ex, 500, Json.MAPPER.createObjectNode().put("error", "Internal server error"));
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        String[] seg = path.split("/");

        if (path.equals("/") || path.equals("/index.html")) {
            require(method, "GET");
            sendRaw(ex, 200, "text/html; charset=utf-8", dashboard);
        } else if (path.equals("/health")) {
            require(method, "GET");
            send(ex, 200, Json.MAPPER.createObjectNode().put("status", "UP").put("workerId", scheduler.workerId()));
        } else if (path.equals("/api/stats")) {
            require(method, "GET");
            send(ex, 200, stats());
        } else if (path.equals("/api/jobs")) {
            switch (method) {
                case "GET" -> send(ex, 200, list(ex));
                case "POST" -> create(ex);
                default -> throw new ApiException(405, "Method not allowed");
            }
        } else if (seg.length == 4 && seg[1].equals("api") && seg[2].equals("jobs")) {
            String id = seg[3];
            switch (method) {
                case "GET" -> send(ex, 200, Json.toJson(scheduler.store().find(id)
                        .orElseThrow(() -> new ApiException(404, "Job not found: " + id))));
                case "DELETE" -> cancel(ex, id);
                default -> throw new ApiException(405, "Method not allowed");
            }
        } else if (seg.length == 5 && seg[1].equals("api") && seg[2].equals("jobs") && seg[4].equals("retry")) {
            require(method, "POST");
            requeue(ex, seg[3]);
        } else {
            throw new ApiException(404, "Not found");
        }
    }

    private ObjectNode stats() {
        ObjectNode root = Json.MAPPER.createObjectNode();
        ObjectNode counts = root.putObject("counts");
        long total = 0;
        for (Map.Entry<JobStatus, Long> e : scheduler.store().countByStatus().entrySet()) {
            counts.put(e.getKey().name(), e.getValue());
            total += e.getValue();
        }
        root.put("total", total);
        root.put("workerId", scheduler.workerId());
        root.put("busyWorkers", scheduler.busyWorkers());
        root.put("executedByThisWorker", scheduler.executedCount());
        root.put("failedAttemptsByThisWorker", scheduler.failedAttempts());
        ArrayNode types = root.putArray("jobTypes");
        knownTypes.forEach(types::add);
        root.put("serverTime", clock.instant().toString());
        return root;
    }

    private ArrayNode list(HttpExchange ex) {
        Map<String, String> query = parseQuery(ex.getRequestURI().getRawQuery());
        JobStatus status = null;
        if (query.containsKey("status") && !query.get("status").isBlank()) {
            try {
                status = JobStatus.valueOf(query.get("status").toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ApiException(400, "Unknown status '" + query.get("status") + "'");
            }
        }
        int limit = 50;
        if (query.containsKey("limit")) {
            try {
                limit = Math.max(1, Math.min(500, Integer.parseInt(query.get("limit"))));
            } catch (NumberFormatException e) {
                throw new ApiException(400, "limit must be a number");
            }
        }
        ArrayNode out = Json.MAPPER.createArrayNode();
        scheduler.store().list(status, limit).forEach(j -> out.add(Json.toJson(j)));
        return out;
    }

    private void create(HttpExchange ex) throws IOException {
        JsonNode body = Json.parse(readBody(ex));
        String type = text(body, "type");
        if (type == null || type.isBlank()) {
            throw new ApiException(400, "'type' is required");
        }
        if (!knownTypes.contains(type)) {
            throw new ApiException(400, "Unknown job type '" + type + "'. Known types: " + knownTypes);
        }
        String cron = text(body, "cron");
        if (cron != null && !CronExpression.isValid(cron)) {
            throw new ApiException(400, "Invalid cron expression '" + cron + "'");
        }
        JsonNode payload = body.has("payload") && !body.get("payload").isNull() ? body.get("payload") : null;
        if (payload != null && !payload.isObject()) {
            throw new ApiException(400, "'payload' must be a JSON object");
        }
        int maxAttempts = body.path("maxAttempts").asInt(NewJob.DEFAULT_MAX_ATTEMPTS);
        Instant runAt = resolveRunAt(body);
        try {
            NewJob request = new NewJob(type, payload == null ? "{}" : payload.toString(), runAt, maxAttempts, cron);
            Job job = scheduler.submit(request);
            ex.getResponseHeaders().add("Location", "/api/jobs/" + job.id());
            send(ex, 201, Json.toJson(job));
        } catch (IllegalArgumentException e) {
            throw new ApiException(400, e.getMessage());
        }
    }

    private Instant resolveRunAt(JsonNode body) {
        if (body.hasNonNull("runAt")) {
            try {
                return Instant.parse(body.get("runAt").asText());
            } catch (DateTimeParseException e) {
                throw new ApiException(400, "'runAt' must be an ISO-8601 instant, e.g. 2026-01-01T10:00:00Z");
            }
        }
        if (body.hasNonNull("delaySeconds")) {
            long seconds = body.get("delaySeconds").asLong();
            if (seconds < 0) {
                throw new ApiException(400, "'delaySeconds' must not be negative");
            }
            return clock.instant().plus(Duration.ofSeconds(seconds));
        }
        return null;
    }

    private void cancel(HttpExchange ex, String id) throws IOException {
        Job job = scheduler.store().find(id).orElseThrow(() -> new ApiException(404, "Job not found: " + id));
        if (!scheduler.store().cancel(id, clock.instant())) {
            throw new ApiException(409, "Only PENDING jobs can be cancelled (job is " + job.status() + ")");
        }
        send(ex, 200, Json.toJson(scheduler.store().find(id).orElseThrow()));
    }

    private void requeue(HttpExchange ex, String id) throws IOException {
        Job job = scheduler.store().find(id).orElseThrow(() -> new ApiException(404, "Job not found: " + id));
        if (!scheduler.store().requeue(id, clock.instant())) {
            throw new ApiException(409, "Only DEAD or CANCELLED jobs can be retried (job is " + job.status() + ")");
        }
        send(ex, 200, Json.toJson(scheduler.store().find(id).orElseThrow()));
    }

    private static String text(JsonNode body, String field) {
        JsonNode n = body.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }

    private static void require(String actual, String expected) {
        if (!expected.equals(actual)) {
            throw new ApiException(405, "Method not allowed");
        }
    }

    private static byte[] readBody(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            byte[] data = in.readNBytes(MAX_BODY_BYTES + 1);
            if (data.length > MAX_BODY_BYTES) {
                throw new ApiException(413, "Request body too large");
            }
            return data;
        }
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> result = new java.util.HashMap<>();
        if (raw == null || raw.isEmpty()) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.put(k, v);
        }
        return result;
    }

    private static void send(HttpExchange ex, int status, JsonNode body) throws IOException {
        sendRaw(ex, status, "application/json", Json.MAPPER.writeValueAsBytes(body));
    }

    private static void sendRaw(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    private static byte[] loadDashboard() throws IOException {
        try (InputStream in = ApiServer.class.getResourceAsStream("/dashboard.html")) {
            if (in == null) {
                throw new IllegalStateException("dashboard.html missing from classpath");
            }
            return in.readAllBytes();
        }
    }
}
