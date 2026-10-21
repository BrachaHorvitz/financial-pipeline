package com.finpipeline.api;

/**
 * Outcome of ingesting one transaction. Used for every batch item and, internally,
 * by the single-ingest endpoint (which maps it to 202 / 409 / 400).
 */
public record BatchItemResult(String deduplicationKey, Status status, String id, String message) {

    public enum Status { ACCEPTED, DUPLICATE, INVALID }

    public static BatchItemResult accepted(String key, String id) {
        return new BatchItemResult(key, Status.ACCEPTED, id, "Transaction queued: " + id);
    }

    public static BatchItemResult duplicate(String key) {
        return new BatchItemResult(key, Status.DUPLICATE, null,
                "Duplicate transaction — key already exists: " + key);
    }

    public static BatchItemResult invalid(String key, String message) {
        return new BatchItemResult(key, Status.INVALID, null, message);
    }
}
