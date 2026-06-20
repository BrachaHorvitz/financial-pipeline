package com.finpipeline.etl;

import com.finpipeline.domain.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class ETLTransformerTest {

    private ETLTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new ETLTransformer();
    }

    // --- Amount normalization ---

    @Test
    @DisplayName("Should scale amount to 2 decimal places")
    void shouldNormalizeAmountScale() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.BANK_API,
                new BigDecimal("100.1"),
                "USD"
        );

        transformer.transform(tx);

        assertThat(tx.getAmount()).isEqualByComparingTo(new BigDecimal("100.10"));
        assertThat(tx.getAmount().scale()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should round amount using HALF_UP")
    void shouldRoundAmountHalfUp() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.BANK_API,
                new BigDecimal("100.555"),
                "USD"
        );

        transformer.transform(tx);

        assertThat(tx.getAmount()).isEqualByComparingTo(new BigDecimal("100.56"));
    }

    // --- Currency normalization ---

    @Test
    @DisplayName("Should uppercase and trim currency")
    void shouldNormalizeCurrency() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.BANK_API,
                new BigDecimal("100.00"),
                "  usd  "
        );

        transformer.transform(tx);

        assertThat(tx.getCurrency()).isEqualTo("USD");
    }

    // --- Metadata enrichment ---

    @Test
    @DisplayName("Should stamp processedAt after transformation")
    void shouldStampProcessedAt() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.BANK_API,
                new BigDecimal("100.00"),
                "USD"
        );

        assertThat(tx.getProcessedAt()).isNull();

        transformer.transform(tx);

        assertThat(tx.getProcessedAt()).isNotNull();
    }

    // --- TAX_AUTHORITY mapping ---

    @Test
    @DisplayName("TAX_AUTHORITY: should convert agorot to ILS (divide by 100)")
    void shouldConvertTaxAuthorityAmountFromAgorot() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.TAX_AUTHORITY,
                new BigDecimal("15000"),
                "ILS"
        );

        transformer.transform(tx);

        assertThat(tx.getAmount()).isEqualByComparingTo(new BigDecimal("150.00"));
        assertThat(tx.getCurrency()).isEqualTo("ILS");
    }

    @Test
    @DisplayName("TAX_AUTHORITY: should force currency to ILS regardless of input")
    void shouldForceTaxAuthorityCurrencyToILS() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.TAX_AUTHORITY,
                new BigDecimal("10000"),
                "USD"
        );

        transformer.transform(tx);

        assertThat(tx.getCurrency()).isEqualTo("ILS");
    }

    // --- BANK_API mapping ---

    @Test
    @DisplayName("BANK_API: should not convert amount (already in major units)")
    void shouldNotConvertBankApiAmount() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.BANK_API,
                new BigDecimal("250.00"),
                "USD"
        );

        transformer.transform(tx);

        assertThat(tx.getAmount()).isEqualByComparingTo(new BigDecimal("250.00"));
    }

    // --- PAYMENT_GATEWAY mapping ---

    @Test
    @DisplayName("PAYMENT_GATEWAY: should convert cents to major currency unit (divide by 100)")
    void shouldConvertPaymentGatewayAmountFromCents() {
        Transaction tx = buildTransaction(
                Transaction.SourceSystem.PAYMENT_GATEWAY,
                new BigDecimal("9999"),
                "USD"
        );

        transformer.transform(tx);

        assertThat(tx.getAmount()).isEqualByComparingTo(new BigDecimal("99.99"));
    }

    // --- helper ---

    private Transaction buildTransaction(
            Transaction.SourceSystem source,
            BigDecimal amount,
            String currency
    ) {
        return Transaction.builder()
                .deduplicationKey("test-key")
                .sourceSystem(source)
                .transactionType(Transaction.TransactionType.PAYMENT)
                .amount(amount)
                .currency(currency)
                .status(Transaction.TransactionStatus.PENDING)
                .retryCount(0)
                .build();
    }
}