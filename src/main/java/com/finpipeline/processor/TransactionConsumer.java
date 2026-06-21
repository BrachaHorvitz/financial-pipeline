package com.finpipeline.processor;

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
 * Ack/nack is handled manually via acknowledgeMode=AUTO (Spring default):
 * - Successful processing or known outcomes (duplicate/DLQ) → message is acked
 * - Unexpected exceptions → message is nacked and requeued up to maxRetries
 *
 * DLQ routing is explicit: we publish to the DLX ourselves on DEAD_LETTER outcome
 * rather than relying solely on RabbitMQ's built-in DLQ, giving us full control
 * over the dead-letter payload and logging.
 */
@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private static final String DLQ_EXCHANGE   = "transaction.dlx";
    private static final String DLQ_ROUTING_KEY = "transaction.dlq";

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
                    routeToDlq(transaction, "Rule failure: " + ruleResult.reason());
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
     * Explicitly publish to the dead-letter exchange.
     * Using RabbitTemplate directly gives us full control over the payload —
     * we can attach headers, metadata, or a failure reason in future iterations.
     */
    private void routeToDlq(Transaction transaction, String reason) {
        log.warn("Routing to DLQ — id={}, reason={}", transaction.getId(), reason);
        transaction.setStatus(TransactionStatus.FAILED);
        rabbitTemplate.convertAndSend(DLQ_EXCHANGE, DLQ_ROUTING_KEY, transaction);
    }
}