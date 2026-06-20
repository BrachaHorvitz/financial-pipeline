package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class AmountValidationRuleTest {

    private AmountValidationRule rule;

    @BeforeEach
    void setUp() {
        rule = new AmountValidationRule();
    }

    @Test
    @DisplayName("Should pass for a valid positive amount")
    void shouldPassForValidAmount() {
        Transaction tx = transactionWithAmount(new BigDecimal("500.00"));

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isTrue();
        assertThat(result.ruleName()).isEqualTo("AMOUNT_VALIDATION");
    }

    @Test
    @DisplayName("Should fail for zero amount")
    void shouldFailForZeroAmount() {
        Transaction tx = transactionWithAmount(BigDecimal.ZERO);

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("positive");
    }

    @Test
    @DisplayName("Should fail for negative amount")
    void shouldFailForNegativeAmount() {
        Transaction tx = transactionWithAmount(new BigDecimal("-100.00"));

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isFalse();
    }

    @Test
    @DisplayName("Should fail for null amount")
    void shouldFailForNullAmount() {
        Transaction tx = transactionWithAmount(null);

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isFalse();
    }

    @Test
    @DisplayName("Should fail for amount exceeding maximum")
    void shouldFailForAmountExceedingMax() {
        Transaction tx = transactionWithAmount(new BigDecimal("1000001.00"));

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("exceeds maximum");
    }

    @Test
    @DisplayName("Should pass for amount exactly at maximum")
    void shouldPassForAmountAtMaximum() {
        Transaction tx = transactionWithAmount(new BigDecimal("1000000.00"));

        RuleResult result = rule.evaluate(tx);

        assertThat(result.passed()).isTrue();
    }

    // --- helper ---
    private Transaction transactionWithAmount(BigDecimal amount) {
        return Transaction.builder()
                .amount(amount)
                .currency("ILS")
                .sourceSystem(Transaction.SourceSystem.BANK_API)
                .transactionType(Transaction.TransactionType.PAYMENT)
                .deduplicationKey("test-key")
                .status(Transaction.TransactionStatus.PENDING)
                .retryCount(0)
                .build();
    }
}