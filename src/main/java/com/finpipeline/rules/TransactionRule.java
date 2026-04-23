package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;

/**
 * Strategy Pattern contract for transaction validation rules.
 * Each rule is a self-contained, independently testable unit.
 */
public interface TransactionRule {

    /**
     * Evaluate the rule against the given transaction.
     * @return RuleResult — contains pass/fail and a reason if failed
     */
    RuleResult evaluate(Transaction transaction);

    /**
     * Human-readable name for logging and audit trails.
     */
    String getRuleName();
}