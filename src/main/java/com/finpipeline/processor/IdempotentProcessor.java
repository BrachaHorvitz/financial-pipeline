package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import com.finpipeline.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Idempotent processing gate — the last checkpoint before a transaction is persisted.
 *
 * IMPORTANT: Transactions arrive deserialized from RabbitMQ JSON — they are DETACHED
 * from any JPA session. All save operations must reload the managed entity from DB first
 * to avoid StaleObjectStateException from Hibernate's optimistic locking.
 */
@Service
public class IdempotentProcessor {

    private static final Logger log = LoggerFactory.getLogger(IdempotentProcessor.class);

    private final TransactionRepository repository;

    @Value("${app.processing.max-retries:3}")
    private int maxRetries;

    public IdempotentProcessor(TransactionRepository repository) {
        this.repository = repository;
    }

    public ProcessingResult process(Transaction incoming) {
        log.debug("IdempotentProcessor — checking id={}, deduplicationKey={}",
                incoming.getId(), incoming.getDeduplicationKey());

        // Reload managed entity — incoming is detached (deserialized from RabbitMQ JSON)
        Transaction managed = repository.findById(incoming.getId()).orElse(null);

        if (managed == null) {
            log.warn("Transaction not found in DB — id={}, treating as new insert",
                    incoming.getId());
            // Fallback: save the incoming directly as a new record
            incoming.setStatus(TransactionStatus.PROCESSED);
            incoming.setProcessedAt(LocalDateTime.now());
            repository.save(incoming);
            return ProcessingResult.success(incoming);
        }

        // --- Stage 0: Redelivery check ---
        // RabbitMQ delivers at-least-once. If this row was already processed, skip it
        // without touching the stored (already ETL-transformed) values.
        // Terminal FAILED rows (retries exhausted) were already dead-lettered; do not DLQ them twice.
        if (managed.getStatus() == TransactionStatus.PROCESSED || isTerminalFailure(managed)) {
            log.info("Redelivery ignored — already handled, id={}, status={}, deduplicationKey={}",
                    managed.getId(), managed.getStatus(), managed.getDeduplicationKey());
            return ProcessingResult.duplicate(managed);
        }

        // Apply ETL-enriched fields from the incoming (detached) object onto managed
        managed.setAmount(incoming.getAmount());
        managed.setCurrency(incoming.getCurrency());
        managed.setProcessedAt(incoming.getProcessedAt());

        // --- Stage 1: Retry limit check ---
        if (hasExceededRetries(managed)) {
            managed.setStatus(TransactionStatus.FAILED);
            repository.save(managed);
            log.warn("Max retries exceeded — id={}, retryCount={}, maxRetries={}",
                    managed.getId(), managed.getRetryCount(), maxRetries);
            return ProcessingResult.deadLetter(managed,
                    "Exceeded max retries (" + maxRetries + ")");
        }

        // --- Stage 2: Happy path ---
        managed.setStatus(TransactionStatus.PROCESSED);
        repository.save(managed);
        log.info("Transaction PROCESSED — id={}, deduplicationKey={}",
                managed.getId(), managed.getDeduplicationKey());
        return ProcessingResult.success(managed);
    }

    /**
     * Persist FAILED for a transaction rejected before reaching {@link #process}
     * (e.g. by the rule engine), so the DB matches what is sent to the DLQ.
     *
     * @return false if the stored row is already PROCESSED and was left untouched — the caller
     *         must then not dead-letter it (a redelivered, already-processed message stays processed).
     */
    public boolean markFailed(Transaction incoming) {
        Transaction managed = repository.findById(incoming.getId()).orElse(null);
        if (managed == null) {
            log.error("Cannot mark failed — transaction not found in DB, id={}", incoming.getId());
            return true;
        }
        if (managed.getStatus() == TransactionStatus.PROCESSED) {
            log.info("markFailed skipped — already PROCESSED, id={}", managed.getId());
            return false;
        }
        managed.setStatus(TransactionStatus.FAILED);
        repository.save(managed);
        return true;
    }

    /**
     * Reload from DB before incrementing retry — incoming is detached.
     */
    public void incrementRetry(Transaction incoming) {
        repository.findById(incoming.getId()).ifPresentOrElse(managed -> {
            if (managed.getStatus() == TransactionStatus.PROCESSED) {
                // A concurrent/redelivered attempt already finished it; never downgrade to FAILED.
                log.info("Retry not incremented — already PROCESSED, id={}", managed.getId());
                return;
            }
            managed.setRetryCount(managed.getRetryCount() + 1);
            managed.setStatus(TransactionStatus.FAILED);
            repository.save(managed);
            log.warn("Retry incremented — id={}, newRetryCount={}",
                    managed.getId(), managed.getRetryCount());
        }, () -> log.error("Cannot increment retry — transaction not found in DB, id={}",
                incoming.getId()));
    }

    private boolean isTerminalFailure(Transaction managed) {
        return managed.getStatus() == TransactionStatus.FAILED && hasExceededRetries(managed);
    }

    private boolean hasExceededRetries(Transaction managed) {
        return managed.getRetryCount() >= maxRetries;
    }
}