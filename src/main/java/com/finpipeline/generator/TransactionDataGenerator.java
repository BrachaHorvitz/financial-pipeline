package com.finpipeline.generator;

import com.finpipeline.broker.TransactionEventPublisher;
import com.finpipeline.domain.Transaction;
import com.finpipeline.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionDataGenerator {

    private final TransactionEventPublisher publisher;
    private final TransactionRepository repository;
    private final Random random = new Random();

    private static final String[] TYPES   = {"PAYMENT", "REFUND", "TRANSFER"};
    private static final String[] SYSTEMS = {"TAX_AUTHORITY", "BANK_API", "PAYMENT_GATEWAY"};
    private static final String[] CURRENCIES = {"ILS", "USD", "EUR"};

    public Transaction generateAndPublish() {
        Transaction tx = Transaction.builder()
                .deduplicationKey(UUID.randomUUID().toString())
                .sourceSystem(SYSTEMS[random.nextInt(SYSTEMS.length)])
                .transactionType(TYPES[random.nextInt(TYPES.length)])
                .amount(BigDecimal.valueOf(random.nextDouble() * 10000)
                        .setScale(2, RoundingMode.HALF_UP))
                .currency(CURRENCIES[random.nextInt(CURRENCIES.length)])
                .status("PENDING")
                .retryCount(0)
                .rawPayload("{\"generated\":true,\"source\":\"mock-generator\"}")
                .build();

        Transaction saved = repository.save(tx);
        publisher.publish(saved);

        log.info("[GENERATOR] Published: {} | {} | {} | {}",
                saved.getId(),
                saved.getSourceSystem(),
                saved.getTransactionType(),
                saved.getAmount());

        return saved;
    }

    public void generateBatch(int count) {
        log.info("[GENERATOR] Generating batch of {} transactions", count);
        for (int i = 0; i < count; i++) {
            generateAndPublish();
        }
        log.info("[GENERATOR] Batch complete");
    }
}