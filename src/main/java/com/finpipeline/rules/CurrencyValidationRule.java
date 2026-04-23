package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Allowlist-based currency check.
 * TAX_AUTHORITY transactions are additionally restricted to ILS only.
 */
@Component
public class CurrencyValidationRule implements TransactionRule {

    private static final Set<String> ALLOWED_CURRENCIES = Set.of("USD", "EUR", "ILS", "GBP");

    @Override
    public RuleResult evaluate(Transaction transaction) {
        String currency = transaction.getCurrency();

        if (currency == null || currency.isBlank()) {
            return RuleResult.fail(getRuleName(), "Currency is missing");
        }

        String normalized = currency.toUpperCase().trim();

        if (!ALLOWED_CURRENCIES.contains(normalized)) {
            return RuleResult.fail(getRuleName(),
                    "Currency '" + normalized + "' is not in the allowed list: " + ALLOWED_CURRENCIES);
        }

        // Source-system-specific restriction
        if (transaction.getSourceSystem() != null &&
                transaction.getSourceSystem().name().equals("TAX_AUTHORITY") &&
                !normalized.equals("ILS")) {
            return RuleResult.fail(getRuleName(),
                    "TAX_AUTHORITY transactions must use ILS, got: " + normalized);
        }

        return RuleResult.pass(getRuleName());
    }

    @Override
    public String getRuleName() {
        return "CURRENCY_VALIDATION";
    }
}