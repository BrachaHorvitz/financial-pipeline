package com.finpipeline.processor;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import com.finpipeline.etl.ETLTransformer;
import com.finpipeline.rules.RuleEngine;
import com.finpipeline.rules.RuleResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class TransactionConsumerTest {

    private RuleEngine ruleEngine;
    private ETLTransformer etl;
    private IdempotentProcessor processor;
    private RabbitTemplate rabbitTemplate;
    private TransactionConsumer consumer;

    @BeforeEach
    void setUp() {
        ruleEngine = mock(RuleEngine.class);
        etl = mock(ETLTransformer.class);
        processor = mock(IdempotentProcessor.class);
        rabbitTemplate = mock(RabbitTemplate.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        consumer = new TransactionConsumer(ruleEngine, etl, processor, rabbitTemplate,
                Counter.builder("p").register(registry),
                Counter.builder("f").register(registry),
                Counter.builder("d").register(registry),
                Counter.builder("q").register(registry),
                registry.timer("t"));
    }

    private Transaction tx() {
        return Transaction.builder().id("id-1").deduplicationKey("k")
                .status(TransactionStatus.PENDING).retryCount(0).build();
    }

    @Test
    @DisplayName("Rule rejection is persisted as FAILED and sent to the DLX with the bound routing key")
    void ruleRejectionGoesToDlq() {
        Transaction tx = tx();
        when(ruleEngine.evaluate(tx)).thenReturn(RuleResult.fail("R", "bad"));

        consumer.consume(tx);

        verify(processor).markFailed(tx);
        verify(rabbitTemplate).convertAndSend(
                RabbitMQConfig.DEAD_LETTER_EXCHANGE, RabbitMQConfig.DLQ_ROUTING_KEY, tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.FAILED);
        verifyNoInteractions(etl);
    }

    @Test
    @DisplayName("Unexpected error increments retry and rethrows so the listener retry / DLX applies")
    void unexpectedErrorRethrows() {
        Transaction tx = tx();
        when(ruleEngine.evaluate(tx)).thenReturn(RuleResult.pass("ALL"));
        when(processor.process(tx)).thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> consumer.consume(tx)).isInstanceOf(RuntimeException.class);

        verify(processor).incrementRetry(tx);
    }
}
