package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Rejects transactions with zero, negative, or suspiciously large amounts.
 * Threshold is intentionally conservative — real systems would load this from config.
 */
@Component
public class AmountValidationRule implements TransactionRule {

    private static final BigDecimal MAX_ALLOWED = new BigDecimal("1000000");

    @Override
    public RuleResult evaluate(Transaction transaction) {
        BigDecimal amount = transaction.getAmount();

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            return RuleResult.fail(getRuleName(), "Amount must be positive, got: " + amount);
        }
        if (amount.compareTo(MAX_ALLOWED) > 0) {
            return RuleResult.fail(getRuleName(),
                    "Amount " + amount + " exceeds maximum allowed " + MAX_ALLOWED);
        }
        return RuleResult.pass(getRuleName());
    }

    @Override
    public String getRuleName() {
        return "AMOUNT_VALIDATION";
    }
}