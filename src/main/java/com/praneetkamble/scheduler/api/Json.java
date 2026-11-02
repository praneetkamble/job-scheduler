package com.praneetkamble.scheduler.api;

import com.praneetkamble.scheduler.model.Job;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;

/** Shared JSON mapper and the API representation of a {@link Job}. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }

    public static ObjectNode toJson(Job job) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("id", job.id());
        n.put("type", job.type());
        try {
            n.set("payload", MAPPER.readTree(job.payload()));
        } catch (IOException e) {
            n.put("payload", job.payload());
        }
        n.put("status", job.status().name());
        n.put("runAt", job.runAt().toString());
        n.put("attempts", job.attempts());
        n.put("maxAttempts", job.maxAttempts());
        n.put("cron", job.cron());
        n.put("runCount", job.runCount());
        n.put("lastError", job.lastError());
        n.put("lockedBy", job.lockedBy());
        n.put("createdAt", job.createdAt().toString());
        n.put("updatedAt", job.updatedAt().toString());
        return n;
    }

    public static JsonNode parse(byte[] body) {
        try {
            JsonNode node = MAPPER.readTree(body);
            if (node == null || !node.isObject()) {
                throw new ApiException(400, "Request body must be a JSON object");
            }
            return node;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new ApiException(400, "Malformed JSON: " + e.getOriginalMessage());
        } catch (IOException e) {
            throw new ApiException(400, "Could not read request body");
        }
    }
}
