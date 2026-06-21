package com.finpipeline.api;

import com.finpipeline.broker.TransactionEventPublisher;
import com.finpipeline.domain.Transaction;
import com.finpipeline.generator.TransactionDataGenerator;
import com.finpipeline.repository.TransactionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@WebFluxTest(TransactionController.class)
class TransactionControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private TransactionRepository repository;

    @MockitoBean
    private TransactionEventPublisher publisher;

    @MockitoBean
    private TransactionDataGenerator generator;

    private static final String VALID_BODY = """
            {
              "deduplicationKey": "test-key-001",
              "sourceSystem": "BANK_API",
              "transactionType": "PAYMENT",
              "amount": 500.00,
              "currency": "ILS",
              "rawPayload": "{}"
            }
            """;

    @Test
    @DisplayName("Should return 202 Accepted for a new valid transaction")
    void shouldReturn202ForNewTransaction() {
        // Arrange
        when(repository.findByDeduplicationKey("test-key-001"))
                .thenReturn(Optional.empty()); // לא קיים — transaction חדשה

        Transaction saved = Transaction.builder()
                .id("generated-id-123")
                .deduplicationKey("test-key-001")
                .status(Transaction.TransactionStatus.PENDING)
                .retryCount(0)
                .build();

        when(repository.save(any())).thenReturn(saved);

        // Act + Assert
        webTestClient.post()
                .uri("/api/v1/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(VALID_BODY)
                .exchange()
                .expectStatus().isAccepted() // 202
                .expectBody(String.class)
                .value(body -> org.assertj.core.api.Assertions
                        .assertThat(body).contains("generated-id-123"));

        verify(publisher, times(1)).publishAsync(saved);
    }

    @Test
    @DisplayName("Should return 409 Conflict for duplicate deduplicationKey")
    void shouldReturn409ForDuplicateKey() {
        // Arrange
        Transaction existing = Transaction.builder()
                .id("existing-id")
                .deduplicationKey("test-key-001")
                .status(Transaction.TransactionStatus.PROCESSED)
                .retryCount(0)
                .build();

        when(repository.findByDeduplicationKey("test-key-001"))
                .thenReturn(Optional.of(existing)); // כבר קיים

        // Act + Assert
        webTestClient.post()
                .uri("/api/v1/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(VALID_BODY)
                .exchange()
                .expectStatus().isEqualTo(409) // Conflict
                .expectBody(String.class)
                .value(body -> org.assertj.core.api.Assertions
                        .assertThat(body).contains("test-key-001"));

        verify(publisher, never()).publishAsync(any());
        verify(repository, never()).save(any());
    }
}