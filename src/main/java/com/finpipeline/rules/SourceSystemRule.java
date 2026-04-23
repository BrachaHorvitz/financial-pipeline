package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.TransactionType;
import com.finpipeline.domain.Transaction.SourceSystem;
import org.springframework.stereotype.Component;

/**
 * Enforces business constraints per source system.
 * Example: BANK_API may not submit REFUND transactions — those come from PAYMENT_GATEWAY only.
 */
@Component
public class SourceSystemRule implements TransactionRule {

    @Override
    public RuleResult evaluate(Transaction transaction) {
        SourceSystem source = transaction.getSourceSystem();
        TransactionType type = transaction.getTransactionType();

        if (source == null) {
            return RuleResult.fail(getRuleName(), "Source system is null");
        }
        if (type == null) {
            return RuleResult.fail(getRuleName(), "Transaction type is null");
        }

        if (source == SourceSystem.BANK_API && type == TransactionType.REFUND) {
            return RuleResult.fail(getRuleName(),
                    "BANK_API is not permitted to submit REFUND transactions");
        }

        if (source == SourceSystem.TAX_AUTHORITY && type == TransactionType.TRANSFER) {
            return RuleResult.fail(getRuleName(),
                    "TAX_AUTHORITY is not permitted to submit TRANSFER transactions");
        }

        return RuleResult.pass(getRuleName());
    }

    @Override
    public String getRuleName() {
        return "SOURCE_SYSTEM_RULE";
    }
}