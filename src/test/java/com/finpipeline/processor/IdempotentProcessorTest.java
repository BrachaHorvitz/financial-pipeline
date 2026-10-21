package com.finpipeline.processor;

import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import com.finpipeline.repository.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class IdempotentProcessorTest {

    private TransactionRepository repository;
    private IdempotentProcessor processor;

    @BeforeEach
    void setUp() {
        repository = mock(TransactionRepository.class);
        processor = new IdempotentProcessor(repository);
        ReflectionTestUtils.setField(processor, "maxRetries", 3);
    }

    private Transaction tx(TransactionStatus status, int retries) {
        return Transaction.builder().id("id-1").deduplicationKey("k")
                .amount(new BigDecimal("10.00")).currency("ILS")
                .status(status).retryCount(retries).build();
    }

    @Test
    @DisplayName("Pending transaction is marked PROCESSED")
    void processesPendingTransaction() {
        Transaction stored = tx(TransactionStatus.PENDING, 0);
        when(repository.findById("id-1")).thenReturn(Optional.of(stored));

        ProcessingResult result = processor.process(tx(TransactionStatus.PENDING, 0));

        assertThat(result.isSuccess()).isTrue();
        assertThat(stored.getStatus()).isEqualTo(TransactionStatus.PROCESSED);
        verify(repository).save(stored);
    }

    @Test
    @DisplayName("Redelivery of an already PROCESSED transaction is a duplicate and changes nothing")
    void redeliveryOfProcessedIsDuplicate() {
        Transaction stored = tx(TransactionStatus.PROCESSED, 0);
        when(repository.findById("id-1")).thenReturn(Optional.of(stored));

        Transaction redelivered = tx(TransactionStatus.PENDING, 0);
        redelivered.setAmount(new BigDecimal("999.00"));
        ProcessingResult result = processor.process(redelivered);

        assertThat(result.isDuplicate()).isTrue();
        assertThat(stored.getAmount()).isEqualByComparingTo("10.00");
        verify(repository, never()).save(any());
    }

    @Test
    @DisplayName("Exceeded retries dead-letters and persists FAILED")
    void exceededRetriesDeadLetters() {
        Transaction stored = tx(TransactionStatus.FAILED, 3);
        when(repository.findById("id-1")).thenReturn(Optional.of(stored));

        ProcessingResult result = processor.process(tx(TransactionStatus.PENDING, 3));

        assertThat(result.isDeadLetter()).isTrue();
        assertThat(stored.getStatus()).isEqualTo(TransactionStatus.FAILED);
        verify(repository).save(stored);
    }

    @Test
    @DisplayName("markFailed persists FAILED on the managed entity")
    void markFailedPersists() {
        Transaction stored = tx(TransactionStatus.PENDING, 0);
        when(repository.findById("id-1")).thenReturn(Optional.of(stored));

        processor.markFailed(tx(TransactionStatus.PENDING, 0));

        assertThat(stored.getStatus()).isEqualTo(TransactionStatus.FAILED);
        verify(repository).save(stored);
    }
}
