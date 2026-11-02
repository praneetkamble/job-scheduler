package com.praneetkamble.scheduler.handler;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Small, dependency-free handlers that make the scheduler useful out of the box. */
public final class BuiltInHandlers {

    private static final Logger log = LoggerFactory.getLogger(BuiltInHandlers.class);

    private BuiltInHandlers() {
    }

    /** Logs {@code payload.message}. */
    public static JobHandler log() {
        return ctx -> log.info("[job {}] {}", ctx.job().id(), ctx.payload().path("message").asText("(no message)"));
    }

    /** Sleeps for {@code payload.millis} milliseconds; useful to observe concurrency. */
    public static JobHandler sleep() {
        return ctx -> Thread.sleep(ctx.payload().path("millis").asLong(1000));
    }

    /** Fails the first {@code payload.failTimes} attempts, then succeeds; demonstrates retry/backoff. */
    public static JobHandler flaky() {
        return ctx -> {
            int failTimes = ctx.payload().path("failTimes").asInt(2);
            if (ctx.attempt() <= failTimes) {
                throw new IllegalStateException("Simulated failure on attempt " + ctx.attempt() + " of " + (failTimes + 1));
            }
            log.info("[job {}] succeeded on attempt {}", ctx.job().id(), ctx.attempt());
        };
    }

    /**
     * Calls a webhook: {@code {"url": "...", "method": "POST", "body": "..."}}. Any non-2xx response is a
     * failure, so the scheduler's retry policy applies to flaky endpoints too.
     */
    public static JobHandler http() {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        return ctx -> {
            JsonNode p = ctx.payload();
            String url = p.path("url").asText(null);
            if (url == null) {
                throw new IllegalArgumentException("payload.url is required");
            }
            String method = p.path("method").asText("GET").toUpperCase();
            HttpRequest.BodyPublisher body = p.hasNonNull("body")
                    ? HttpRequest.BodyPublishers.ofString(p.get("body").asText())
                    : HttpRequest.BodyPublishers.noBody();
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .method(method, body)
                    .build();
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException(method + " " + url + " returned HTTP " + response.statusCode());
            }
        };
    }
}
