package com.praneetkamble.scheduler.handler;

import com.praneetkamble.scheduler.model.Job;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/** What a {@link JobHandler} gets to see about the job it is executing. */
public record JobContext(Job job) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 1-based number of the current attempt (1 on the first try). */
    public int attempt() {
        return job.attempts();
    }

    /** The job payload parsed as JSON. */
    public JsonNode payload() {
        try {
            return MAPPER.readTree(job.payload());
        } catch (IOException e) {
            throw new IllegalArgumentException("Job payload is not valid JSON", e);
        }
    }
}
