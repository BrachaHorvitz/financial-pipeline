package com.finpipeline.processor;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import com.finpipeline.etl.ETLTransformer;
import com.finpipeline.rules.RuleEngine;
import com.finpipeline.rules.RuleResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ consumer — the pipeline orchestrator.
 *
 * Listens on transaction.queue and runs each message through:
 *   RuleEngine → ETLTransformer → IdempotentProcessor
 *
 * Ack/nack uses acknowledgeMode=AUTO (Spring default):
 * - Successful processing or known outcomes (duplicate/DLQ) → message is acked
 * - Unexpected exceptions → the listener retries (spring.rabbitmq.listener.simple.retry);
 *   once attempts are exhausted the message is handed to the recoverer, which
 *   (see {@link DeadLetterRecoverer}) dead-letters it through {@link #recoverExhausted}
 *
 * DLQ routing is always explicit: every dead-letter path (rule rejection, DEAD_LETTER
 * outcome, exhausted listener retries) goes through routeToDlq, so the DLQ payload
 * (status FAILED), the DB state and the metrics stay consistent. The queue's own
 * x-dead-letter arguments remain only as a safety net for messages the broker rejects.
 */
@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final RuleEngine ruleEngine;
    private final ETLTransformer etlTransformer;
    private final IdempotentProcessor idempotentProcessor;
    private final RabbitTemplate rabbitTemplate;

    // --- Metrics ---
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;
    private final Timer processingTimer;

    public TransactionConsumer(
            RuleEngine ruleEngine,
            ETLTransformer etlTransformer,
            IdempotentProcessor idempotentProcessor,
            RabbitTemplate rabbitTemplate,
            Counter processedCounter,
            Counter failedCounter,
            Counter duplicateCounter,
            Counter dlqCounter,
            Timer processingTimer
    ) {
        this.ruleEngine           = ruleEngine;
        this.etlTransformer       = etlTransformer;
        this.idempotentProcessor  = idempotentProcessor;
        this.rabbitTemplate       = rabbitTemplate;
        this.processedCounter     = processedCounter;
        this.failedCounter        = failedCounter;
        this.duplicateCounter     = duplicateCounter;
        this.dlqCounter           = dlqCounter;
        this.processingTimer      = processingTimer;
    }

    @RabbitListener(queues = "transaction.queue")
    public void consume(Transaction transaction) {
        log.info("Consumed message — id={}, source={}, type={}, status={}",
                transaction.getId(),
                transaction.getSourceSystem(),
                transaction.getTransactionType(),
                transaction.getStatus());

        // Timer wraps the entire pipeline — measures end-to-end duration
        processingTimer.record(() -> {
            try {
                // --- Stage 1: Rule Engine ---
                RuleResult ruleResult = ruleEngine.evaluate(transaction);

                if (ruleResult.failed()) {
                    log.warn("Transaction rejected by rules — id={}, reason={}",
                            transaction.getId(), ruleResult.reason());
                    failAndDeadLetter(transaction, "Rule failure: " + ruleResult.reason());
                    return;
                }

                // --- Stage 2: ETL Transformation ---
                etlTransformer.transform(transaction);

                // --- Stage 3: Idempotent Processing ---
                ProcessingResult result = idempotentProcessor.process(transaction);

                switch (result.outcome()) {
                    case SUCCESS -> {
                        log.info("Pipeline SUCCESS — id={}", transaction.getId());
                        processedCounter.increment();
                    }
                    case DUPLICATE -> {
                        log.info("Pipeline DUPLICATE — id={}", transaction.getId());
                        duplicateCounter.increment();
                    }
                    case DEAD_LETTER -> {
                        log.warn("Pipeline DEAD_LETTER — id={}, reason={}",
                                transaction.getId(), result.reason());
                        dlqCounter.increment();
                        routeToDlq(transaction, result.reason());
                    }
                }

            } catch (Exception e) {
                log.error("Unexpected error processing id={} — incrementing retry. Error: {}",
                        transaction.getId(), e.getMessage(), e);
                failedCounter.increment();
                idempotentProcessor.incrementRetry(transaction);
                throw new RuntimeException("Pipeline failure for id=" + transaction.getId(), e);
            }
        });
    }

    /**
     * Called by {@link DeadLetterRecoverer} once listener retries are exhausted.
     */
    public void recoverExhausted(Transaction transaction, Throwable cause) {
        if (failAndDeadLetter(transaction, "Listener retries exhausted: " + cause.getMessage())) {
            dlqCounter.increment();
        }
    }

    /**
     * Persist FAILED and dead-letter — unless the stored row is already PROCESSED
     * (a redelivery of a finished transaction must not be failed or dead-lettered).
     */
    private boolean failAndDeadLetter(Transaction transaction, String reason) {
        if (!idempotentProcessor.markFailed(transaction)) {
            log.info("Skipping DLQ — already PROCESSED, id={}", transaction.getId());
            return false;
        }
        routeToDlq(transaction, reason);
        return true;
    }

    /**
     * Explicitly publish to the dead-letter exchange.
     * Using RabbitTemplate directly gives us full control over the payload —
     * we can attach headers, metadata, or a failure reason in future iterations.
     */
    private void routeToDlq(Transaction transaction, String reason) {
        log.warn("Routing to DLQ — id={}, reason={}", transaction.getId(), reason);
        transaction.setStatus(TransactionStatus.FAILED);
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DEAD_LETTER_EXCHANGE,
                RabbitMQConfig.DLQ_ROUTING_KEY,
                transaction);
    }
}