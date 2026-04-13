package com.finpipeline.broker;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventPublisher {

    private final RabbitTemplate rabbitTemplate;

    public void publish(Transaction transaction) {
        log.info("[BROKER] Publishing transaction {} from {} to queue",
                transaction.getId(), transaction.getSourceSystem());

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.TRANSACTION_EXCHANGE,
                RabbitMQConfig.ROUTING_KEY,
                transaction
        );
    }
}