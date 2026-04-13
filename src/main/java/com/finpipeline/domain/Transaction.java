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

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false, unique = true)
    private String deduplicationKey;

    private String sourceSystem;      // TAX_AUTHORITY, BANK_API, PAYMENT_GATEWAY
    private String transactionType;   // PAYMENT, REFUND, TRANSFER
    private BigDecimal amount;
    private String currency;
    private String status;            // PENDING, PROCESSED, FAILED, DUPLICATE
    private String rawPayload;

    @CreationTimestamp
    private LocalDateTime receivedAt;
    private LocalDateTime processedAt;
    private Integer retryCount;
}