package com.finpipeline.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    public enum SourceSystem {
        TAX_AUTHORITY, BANK_API, PAYMENT_GATEWAY
    }

    public enum TransactionType {
        PAYMENT, REFUND, TRANSFER
    }

    public enum TransactionStatus {
        PENDING, PROCESSED, FAILED, DUPLICATE
    }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false, unique = true)
    private String deduplicationKey;

    @Enumerated(EnumType.STRING)
    private SourceSystem sourceSystem;

    @Enumerated(EnumType.STRING)
    private TransactionType transactionType;

    private BigDecimal amount;
    private String currency;

    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    private String rawPayload;

    @CreationTimestamp
    private LocalDateTime receivedAt;
    private LocalDateTime processedAt;
    private Integer retryCount;

    /**
     * Optimistic locking version field.
     * Hibernate increments this on every update.
     * If two threads try to update the same row concurrently,
     * the second one gets OptimisticLockException — not silent data corruption.
     */
    @Version
    private Long version;
}