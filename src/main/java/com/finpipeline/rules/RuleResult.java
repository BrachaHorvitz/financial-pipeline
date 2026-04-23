package com.finpipeline.rules;

/**
 * Immutable result of a single rule evaluation.
 * Using a record keeps this concise and naturally immutable.
 */
public record RuleResult(boolean passed, String ruleName, String reason) {

    public static RuleResult pass(String ruleName) {
        return new RuleResult(true, ruleName, null);
    }

    public static RuleResult fail(String ruleName, String reason) {
        return new RuleResult(false, ruleName, reason);
    }

    public boolean failed() {
        return !passed;
    }
}