package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeadLetterRecovererTest {

    @Test
    @DisplayName("Recoverer deserializes the failed message and dead-letters it via the consumer")
    void recoversTransactionFromMessage() {
        TransactionConsumer consumer = mock(TransactionConsumer.class);
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        DeadLetterRecoverer recoverer = new DeadLetterRecoverer(consumer, converter);

        Transaction tx = Transaction.builder().id("id-1").deduplicationKey("k")
                .amount(new BigDecimal("10.00")).currency("ILS").retryCount(0).build();
        Message message = converter.toMessage(tx, new org.springframework.amqp.core.MessageProperties());
        // Simulate a consumer-side message: no usable type header.
        message.getMessageProperties().getHeaders().remove("__TypeId__");

        Throwable cause = new IllegalStateException("db down");
        recoverer.recover(message, new RuntimeException("wrapper", cause));

        org.mockito.ArgumentCaptor<Transaction> captor =
                org.mockito.ArgumentCaptor.forClass(Transaction.class);
        verify(consumer).recoverExhausted(captor.capture(), eq(cause));
        assertThat(captor.getValue().getId()).isEqualTo("id-1");
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("10.00");
    }
}
