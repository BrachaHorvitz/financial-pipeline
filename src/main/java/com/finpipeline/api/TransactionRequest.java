package com.finpipeline.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

public record TransactionRequest(
        @NotBlank String deduplicationKey,
        @NotBlank String sourceSystem,
        @NotBlank String transactionType,
        @NotNull @Positive BigDecimal amount,
        @NotBlank String currency,
        String rawPayload
) {}