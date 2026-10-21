package com.finpipeline.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitMQConfigTest {

    private final RabbitMQConfig config = new RabbitMQConfig();

    @Test
    @DisplayName("DLQ is bound to the DLX with the routing key used for dead-lettering")
    void deadLetterBindingMatchesRoutingKey() {
        Binding binding = config.deadLetterBinding();

        assertThat(binding.getExchange()).isEqualTo(RabbitMQConfig.DEAD_LETTER_EXCHANGE);
        assertThat(binding.getDestination()).isEqualTo(RabbitMQConfig.DEAD_LETTER_QUEUE);
        assertThat(binding.getRoutingKey()).isEqualTo(RabbitMQConfig.DLQ_ROUTING_KEY);
    }

    @Test
    @DisplayName("Main queue dead-letters to the same exchange and key the DLQ is bound with")
    void mainQueueDeadLettersToDlqBinding() {
        Queue queue = config.transactionQueue();

        assertThat(queue.getArguments())
                .containsEntry("x-dead-letter-exchange", RabbitMQConfig.DEAD_LETTER_EXCHANGE)
                .containsEntry("x-dead-letter-routing-key", RabbitMQConfig.DLQ_ROUTING_KEY);
    }
}
