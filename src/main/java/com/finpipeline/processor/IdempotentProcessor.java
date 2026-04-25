package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import com.finpipeline.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Idempotent processing gate — the last checkpoint before a transaction is persisted.
 *
 * Responsibilities:
 *   1. Deduplication — reject transactions whose deduplicationKey already exists in DB
 *   2. Retry tracking — increment retryCount on failure, mark FAILED when max is exceeded
 *   3. DLQ routing signal — returns ProcessingResult so the caller decides on DLQ
 *
 * This class is intentionally free of RabbitMQ imports — it returns a result,
 * it does not send to queues itself. That keeps it unit-testable without a broker.
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

    /**
     * Main entry point. Call this after RuleEngine + ETLTransformer have run.
     *
     * @return ProcessingResult — tells the caller whether to persist, skip, or DLQ
     */
    public ProcessingResult process(Transaction transaction) {
        log.debug("IdempotentProcessor — checking transaction id={}, deduplicationKey={}",
                transaction.getId(), transaction.getDeduplicationKey());

        // --- Stage 1: Deduplication check ---
        if (isDuplicate(transaction)) {
            transaction.setStatus(TransactionStatus.DUPLICATE);
            repository.save(transaction);
            log.info("DUPLICATE detected — deduplicationKey={}, id={}",
                    transaction.getDeduplicationKey(), transaction.getId());
            return ProcessingResult.duplicate(transaction);
        }

        // --- Stage 2: Retry limit check ---
        if (hasExceededRetries(transaction)) {
            transaction.setStatus(TransactionStatus.FAILED);
            repository.save(transaction);
            log.warn("Max retries exceeded — id={}, retryCount={}, maxRetries={}",
                    transaction.getId(), transaction.getRetryCount(), maxRetries);
            return ProcessingResult.deadLetter(transaction,
                    "Exceeded max retries (" + maxRetries + ")");
        }

        // --- Stage 3: Happy path — mark PROCESSED and persist ---
        transaction.setStatus(TransactionStatus.PROCESSED);
        repository.save(transaction);
        log.info("Transaction PROCESSED — id={}, deduplicationKey={}",
                transaction.getId(), transaction.getDeduplicationKey());
        return ProcessingResult.success(transaction);
    }

    /**
     * Increments retryCount and persists. Called by the consumer on processing exceptions.
     * Separated from process() so retry state is updated even when the pipeline throws.
     */
    public void incrementRetry(Transaction transaction) {
        transaction.setRetryCount(transaction.getRetryCount() + 1);
        transaction.setStatus(TransactionStatus.FAILED);
        repository.save(transaction);
        log.warn("Retry incremented — id={}, newRetryCount={}",
                transaction.getId(), transaction.getRetryCount());
    }

    private boolean isDuplicate(Transaction transaction) {
        return repository.findByDeduplicationKey(transaction.getDeduplicationKey())
                .map(existing -> !existing.getId().equals(transaction.getId()))
                .orElse(false);
    }

    private boolean hasExceededRetries(Transaction transaction) {
        return transaction.getRetryCount() >= maxRetries;
    }
}