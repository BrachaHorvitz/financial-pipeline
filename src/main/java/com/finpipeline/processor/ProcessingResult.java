package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;

/**
 * Outcome of IdempotentProcessor.process().
 *
 * The consumer uses this to decide whether to ack, nack, or route to DLQ —
 * without the processor needing to know anything about RabbitMQ.
 */
public record ProcessingResult(
        Outcome outcome,
        Transaction transaction,
        String reason
) {

    public enum Outcome {
        SUCCESS,
        DUPLICATE,
        DEAD_LETTER
    }

    public static ProcessingResult success(Transaction transaction) {
        return new ProcessingResult(Outcome.SUCCESS, transaction, null);
    }

    public static ProcessingResult duplicate(Transaction transaction) {
        return new ProcessingResult(Outcome.DUPLICATE, transaction, "Duplicate deduplication key");
    }

    public static ProcessingResult deadLetter(Transaction transaction, String reason) {
        return new ProcessingResult(Outcome.DEAD_LETTER, transaction, reason);
    }

    public boolean isSuccess()    { return outcome == Outcome.SUCCESS; }
    public boolean isDuplicate()  { return outcome == Outcome.DUPLICATE; }
    public boolean isDeadLetter() { return outcome == Outcome.DEAD_LETTER; }
}