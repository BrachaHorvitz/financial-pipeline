package com.finpipeline.broker;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventPublisher {

    private final RabbitTemplate rabbitTemplate;

    /**
     * Synchronous publish — used internally when caller needs to know
     * the message was sent before continuing (e.g. batch processing).
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
     * Exceptions are caught by AsyncUncaughtExceptionHandler in ConcurrencyConfig.
     *
     * Use this for single ingest endpoints where response latency matters.
     */
    @Async("pipelineExecutor")
    public CompletableFuture<Void> publishAsync(Transaction transaction) {
        log.info("[BROKER] Async publishing transaction {} on thread {}",
                transaction.getId(),
                Thread.currentThread().getName());

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.TRANSACTION_EXCHANGE,
                RabbitMQConfig.ROUTING_KEY,
                transaction
        );

        return CompletableFuture.completedFuture(null);
    }
}