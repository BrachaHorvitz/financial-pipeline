package com.finpipeline.broker;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventPublisher {

    private static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private final RabbitTemplate rabbitTemplate;

    /**
     * Synchronous publish — used when caller must confirm delivery before continuing.
     * Used by TransactionDataGenerator for batch publishing.
     */
    public void publish(Transaction transaction) {
        log.info("[BROKER] Publishing transaction {} from {} to queue",
                transaction.getId(), transaction.getSourceSystem());

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.TRANSACTION_EXCHANGE,
                RabbitMQConfig.ROUTING_KEY,
                transaction
        );
    }

    /**
     * Async publish — runs on pipeline-executor thread pool.
     * Caller returns immediately without waiting for RabbitMQ confirmation.
     *
     * Timeout: if RabbitMQ does not accept the message within 5 seconds,
     * the task is cancelled and the failure is logged by AsyncUncaughtExceptionHandler.
     */
    @Async("pipelineExecutor")
    public CompletableFuture<Void> publishAsync(Transaction transaction) {
        log.info("[BROKER] Async publishing transaction {} on thread {}",
                transaction.getId(),
                Thread.currentThread().getName());

        return CompletableFuture
                .runAsync(() -> rabbitTemplate.convertAndSend(
                        RabbitMQConfig.TRANSACTION_EXCHANGE,
                        RabbitMQConfig.ROUTING_KEY,
                        transaction
                ))
                .orTimeout(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    if (ex instanceof TimeoutException) {
                        log.error("[BROKER] Publish TIMED OUT after {}s — transaction id={}",
                                PUBLISH_TIMEOUT_SECONDS, transaction.getId());
                    } else {
                        log.error("[BROKER] Publish FAILED — transaction id={}, error={}",
                                transaction.getId(), ex.getMessage(), ex);
                    }
                    return null;
                });
    }
}