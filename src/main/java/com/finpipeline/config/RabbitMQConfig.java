package com.finpipeline.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // Queue names
    public static final String TRANSACTION_QUEUE    = "transaction.queue";
    public static final String DEAD_LETTER_QUEUE    = "transaction.dlq";

    // Exchange names
    public static final String TRANSACTION_EXCHANGE = "transaction.exchange";
    public static final String DEAD_LETTER_EXCHANGE = "transaction.dlx";

    // Routing keys
    public static final String ROUTING_KEY          = "transaction.received";
    public static final String DLQ_ROUTING_KEY      = "transaction.failed";

    // --- Dead Letter Exchange + Queue ---
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DEAD_LETTER_EXCHANGE);
    }

    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder
                .bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DLQ_ROUTING_KEY);
    }

    // --- Main Exchange + Queue (with DLQ wired in) ---
    @Bean
    public DirectExchange transactionExchange() {
        return new DirectExchange(TRANSACTION_EXCHANGE);
    }

    @Bean
    public Queue transactionQueue() {
        return QueueBuilder.durable(TRANSACTION_QUEUE)
                .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding transactionBinding() {
        return BindingBuilder
                .bind(transactionQueue())
                .to(transactionExchange())
                .with(ROUTING_KEY);
    }

    // --- JSON serialization ---
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
}