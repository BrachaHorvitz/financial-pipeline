package com.finpipeline.integration;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient.ResponseSpec;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * End-to-end: HTTP ingest → PostgreSQL → RabbitMQ → consumer → PostgreSQL / DLQ.
 * Each test uses a fresh deduplication key, so assertions never depend on other tests' data.
 */
class TransactionPipelineIntegrationTest extends AbstractIntegrationTest {

    private static final Duration PIPELINE_TIMEOUT = Duration.ofSeconds(15);
    private static final long DLQ_RECEIVE_TIMEOUT_MS = 10_000;

    @Test
    @DisplayName("Valid transaction is persisted, consumed, ETL-transformed and marked PROCESSED")
    void validTransactionIsProcessed() {
        String key = newKey();

        // PAYMENT_GATEWAY sends minor units: 12345 cents → 123.45
        ingest(key, "PAYMENT_GATEWAY", "PAYMENT", "12345", "usd")
                .expectStatus().isAccepted();

        Transaction processed = awaitStatus(key, TransactionStatus.PROCESSED);
        assertThat(processed.getAmount()).isEqualByComparingTo("123.45");
        assertThat(processed.getCurrency()).isEqualTo("USD");
        assertThat(processed.getProcessedAt()).isNotNull();
        assertThat(processed.getRetryCount()).isZero();
    }

    @Test
    @DisplayName("Same key ingested twice: 202 then 409, a single row")
    void duplicateKeyIsRejectedAtIngest() {
        String key = newKey();

        ingest(key, "BANK_API", "PAYMENT", "100", "ILS").expectStatus().isAccepted();
        ingest(key, "BANK_API", "PAYMENT", "100", "ILS").expectStatus().isEqualTo(409);

        assertThat(repository.findAll())
                .filteredOn(tx -> key.equals(tx.getDeduplicationKey()))
                .hasSize(1);
    }

    @Test
    @DisplayName("Concurrent ingests of one key: exactly one 202, the rest 409 (unique constraint race)")
    void concurrentIngestOfSameKeyYieldsOneAccepted() throws Exception {
        String key = newKey();
        int requests = 8;

        List<Callable<HttpStatusCode>> calls = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            calls.add(() -> ingest(key, "BANK_API", "PAYMENT", "100", "ILS")
                    .returnResult(String.class).getStatus());
        }

        List<HttpStatusCode> statuses = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(requests);
        try {
            for (Future<HttpStatusCode> f : pool.invokeAll(calls)) {
                statuses.add(f.get());
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(statuses).filteredOn(s -> s.value() == 202).hasSize(1);
        assertThat(statuses).filteredOn(s -> s.value() == 409).hasSize(requests - 1);
        assertThat(repository.findByDeduplicationKey(key)).isPresent();
    }

    @Test
    @DisplayName("Rule rejection: row marked FAILED and the transaction reaches the DLQ")
    void ruleRejectionIsDeadLettered() {
        String key = newKey();

        // SourceSystemRule: BANK_API may not submit REFUND
        ingest(key, "BANK_API", "REFUND", "100", "ILS").expectStatus().isAccepted();

        Object dead = rabbitTemplate.receiveAndConvert(RabbitMQConfig.DEAD_LETTER_QUEUE, DLQ_RECEIVE_TIMEOUT_MS);
        assertThat(dead).isInstanceOf(Transaction.class);
        assertThat(((Transaction) dead).getDeduplicationKey()).isEqualTo(key);
        assertThat(((Transaction) dead).getStatus()).isEqualTo(TransactionStatus.FAILED);

        awaitStatus(key, TransactionStatus.FAILED);
    }

    @Test
    @DisplayName("Redelivery of a PROCESSED transaction is ignored: no second ETL pass, no DLQ")
    void redeliveryOfProcessedTransactionIsIgnored() {
        String key = newKey();
        ingest(key, "PAYMENT_GATEWAY", "PAYMENT", "12345", "USD").expectStatus().isAccepted();
        Transaction processed = awaitStatus(key, TransactionStatus.PROCESSED);
        double duplicatesBefore = counter("pipeline.transactions.duplicate");

        // Simulate at-least-once redelivery of the original (pre-ETL) message
        rabbitTemplate.convertAndSend(RabbitMQConfig.TRANSACTION_EXCHANGE, RabbitMQConfig.ROUTING_KEY,
                copyOf(processed, new BigDecimal("12345"), "USD"));

        await().atMost(PIPELINE_TIMEOUT)
                .until(() -> counter("pipeline.transactions.duplicate") > duplicatesBefore);

        Transaction after = repository.findByDeduplicationKey(key).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(TransactionStatus.PROCESSED);
        // Divided by 100 once, not twice
        assertThat(after.getAmount()).isEqualByComparingTo("123.45");
        assertThat(rabbitTemplate.receive(RabbitMQConfig.DEAD_LETTER_QUEUE, 1_000)).isNull();
    }

    @Test
    @DisplayName("Redelivery of a PROCESSED transaction that now fails the rules stays PROCESSED")
    void redeliveryFailingRulesDoesNotDowngradeProcessed() {
        String key = newKey();
        ingest(key, "BANK_API", "PAYMENT", "100", "ILS").expectStatus().isAccepted();
        Transaction processed = awaitStatus(key, TransactionStatus.PROCESSED);

        // Same transaction, but with a currency the rule engine rejects
        rabbitTemplate.convertAndSend(RabbitMQConfig.TRANSACTION_EXCHANGE, RabbitMQConfig.ROUTING_KEY,
                copyOf(processed, processed.getAmount(), "XYZ"));

        verify(idempotentProcessor, timeout(PIPELINE_TIMEOUT.toMillis()))
                .markFailed(argThat(tx -> processed.getId().equals(tx.getId())));

        assertThat(rabbitTemplate.receive(RabbitMQConfig.DEAD_LETTER_QUEUE, 1_000)).isNull();
        assertThat(repository.findByDeduplicationKey(key).orElseThrow().getStatus())
                .isEqualTo(TransactionStatus.PROCESSED);
    }

    @Test
    @DisplayName("Persistent failure: listener retries run out, row FAILED with retryCount=3, explicit DLQ")
    void exhaustedRetriesAreDeadLetteredExplicitly() {
        String key = newKey();
        doThrow(new IllegalStateException("simulated downstream outage"))
                .when(etlTransformer).transform(argThat(tx -> tx != null && key.equals(tx.getDeduplicationKey())));
        double dlqBefore = counter("pipeline.transactions.dlq");

        ingest(key, "BANK_API", "PAYMENT", "100", "ILS").expectStatus().isAccepted();

        Object dead = rabbitTemplate.receiveAndConvert(RabbitMQConfig.DEAD_LETTER_QUEUE, DLQ_RECEIVE_TIMEOUT_MS);
        assertThat(dead).isInstanceOf(Transaction.class);
        assertThat(((Transaction) dead).getStatus()).isEqualTo(TransactionStatus.FAILED);

        Transaction failed = awaitStatus(key, TransactionStatus.FAILED);
        assertThat(failed.getRetryCount()).isEqualTo(3);
        await().atMost(PIPELINE_TIMEOUT)
                .until(() -> counter("pipeline.transactions.dlq") == dlqBefore + 1);
    }

    @Test
    @DisplayName("Unparseable message is dead-lettered by the broker instead of being requeued forever")
    void poisonMessageReachesDlq() {
        Message poison = MessageBuilder.withBody("not json".getBytes(StandardCharsets.UTF_8))
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .build();

        rabbitTemplate.send(RabbitMQConfig.TRANSACTION_EXCHANGE, RabbitMQConfig.ROUTING_KEY, poison);

        Message dead = rabbitTemplate.receive(RabbitMQConfig.DEAD_LETTER_QUEUE, DLQ_RECEIVE_TIMEOUT_MS);
        assertThat(dead).isNotNull();
        assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).isEqualTo("not json");
    }

    @Test
    @DisplayName("Batch ingest returns one result per item: ACCEPTED, DUPLICATE and INVALID")
    void batchReturnsPerItemResults() {
        String existing = newKey();
        String fresh = newKey();
        ingest(existing, "BANK_API", "PAYMENT", "100", "ILS").expectStatus().isAccepted();

        String body = """
                [
                  %s,
                  %s,
                  {"deduplicationKey":"%s","sourceSystem":"BANK_API","transactionType":"PAYMENT",
                   "amount":-5,"currency":"ILS"}
                ]
                """.formatted(requestJson(existing, "BANK_API", "PAYMENT", "100", "ILS"),
                requestJson(fresh, "BANK_API", "PAYMENT", "100", "ILS"),
                newKey());

        webTestClient.post().uri("/api/v1/transactions/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(3)
                .jsonPath("$[?(@.deduplicationKey == '%s')].status".formatted(existing)).isEqualTo("DUPLICATE")
                .jsonPath("$[?(@.deduplicationKey == '%s')].status".formatted(fresh)).isEqualTo("ACCEPTED")
                .jsonPath("$[?(@.status == 'INVALID')].message").exists();

        awaitStatus(fresh, TransactionStatus.PROCESSED);
    }

    // --- helpers ---

    private static String newKey() {
        return "it-" + UUID.randomUUID();
    }

    private static String requestJson(String key, String source, String type, String amount, String currency) {
        return """
                {"deduplicationKey":"%s","sourceSystem":"%s","transactionType":"%s",
                 "amount":%s,"currency":"%s","rawPayload":"{}"}
                """.formatted(key, source, type, amount, currency);
    }

    private ResponseSpec ingest(String key, String source, String type, String amount, String currency) {
        return webTestClient.post().uri("/api/v1/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestJson(key, source, type, amount, currency))
                .exchange();
    }

    private Transaction awaitStatus(String key, TransactionStatus expected) {
        return await().atMost(PIPELINE_TIMEOUT)
                .until(() -> repository.findByDeduplicationKey(key).orElse(null),
                        tx -> tx != null && tx.getStatus() == expected);
    }

    private static Transaction copyOf(Transaction source, BigDecimal amount, String currency) {
        return Transaction.builder()
                .id(source.getId())
                .deduplicationKey(source.getDeduplicationKey())
                .sourceSystem(source.getSourceSystem())
                .transactionType(source.getTransactionType())
                .amount(amount)
                .currency(currency)
                .status(TransactionStatus.PENDING)
                .retryCount(0)
                .build();
    }
}
