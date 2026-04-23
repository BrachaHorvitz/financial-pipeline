package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates all registered TransactionRule implementations.
 *
 * Spring automatically injects every @Component that implements TransactionRule
 * into the list — adding a new rule requires zero changes here.
 *
 * Fail-fast: stops at first failing rule. Switch to collecting all failures
 * if you want a full violation report instead.
 */
@Service
public class RuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);

    private final List<TransactionRule> rules;

    public RuleEngine(List<TransactionRule> rules) {
        this.rules = rules;
        log.info("RuleEngine initialized with {} rules: {}",
                rules.size(),
                rules.stream().map(TransactionRule::getRuleName).toList());
    }

    /**
     * Runs all rules in order. Returns the first failure, or a pass result if all rules pass.
     */
    public RuleResult evaluate(Transaction transaction) {
        log.debug("Evaluating {} rules for transaction id={}", rules.size(), transaction.getId());

        for (TransactionRule rule : rules) {
            RuleResult result = rule.evaluate(transaction);

            if (result.failed()) {
                log.warn("Rule FAILED — rule={}, transactionId={}, reason={}",
                        result.ruleName(), transaction.getId(), result.reason());
                return result;
            }

            log.debug("Rule PASSED — rule={}, transactionId={}", result.ruleName(), transaction.getId());
        }

        log.debug("All rules passed for transactionId={}", transaction.getId());
        return RuleResult.pass("ALL_RULES");
    }
}