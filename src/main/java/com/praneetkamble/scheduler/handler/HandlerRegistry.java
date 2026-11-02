package com.praneetkamble.scheduler.handler;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Maps a job {@code type} to its handler. */
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public HandlerRegistry register(String type, JobHandler handler) {
        if (handlers.putIfAbsent(type, handler) != null) {
            throw new IllegalStateException("A handler for type '" + type + "' is already registered");
        }
        return this;
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return Set.copyOf(handlers.keySet());
    }

    /** Registry preloaded with the demo handlers: {@code log}, {@code sleep}, {@code flaky} and {@code http}. */
    public static HandlerRegistry withBuiltIns() {
        return new HandlerRegistry()
                .register("log", BuiltInHandlers.log())
                .register("sleep", BuiltInHandlers.sleep())
                .register("flaky", BuiltInHandlers.flaky())
                .register("http", BuiltInHandlers.http());
    }
}
