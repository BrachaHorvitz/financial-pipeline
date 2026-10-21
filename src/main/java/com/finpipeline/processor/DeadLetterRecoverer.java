package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.stereotype.Component;

/**
 * Invoked by the listener retry interceptor when all attempts have failed.
 * Routes the transaction through the consumer's explicit DLQ path instead of letting the
 * broker dead-letter the raw message, so payload, DB state and metrics match other DLQ paths.
 * Spring Boot picks up a MessageRecoverer bean automatically.
 */
@Component
public class DeadLetterRecoverer implements MessageRecoverer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRecoverer.class);

    private final TransactionConsumer consumer;
    private final Jackson2JsonMessageConverter converter;

    public DeadLetterRecoverer(TransactionConsumer consumer, Jackson2JsonMessageConverter converter) {
        this.consumer = consumer;
        this.converter = converter;
    }

    @Override
    public void recover(Message message, Throwable cause) {
        message.getMessageProperties().setInferredArgumentType(Transaction.class);
        Transaction transaction = (Transaction) converter.fromMessage(message);
        Throwable root = cause.getCause() != null ? cause.getCause() : cause;
        log.error("Listener retries exhausted — id={}", transaction.getId(), root);
        consumer.recoverExhausted(transaction, root);
    }
}
