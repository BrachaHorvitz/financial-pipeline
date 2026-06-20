package com.finpipeline.rules;

import com.finpipeline.domain.Transaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RuleEngineTest {

    // --- helpers ---

    private Transaction buildTransaction() {
        return Transaction.builder()
                .deduplicationKey("test-key")
                .sourceSystem(Transaction.SourceSystem.BANK_API)
                .transactionType(Transaction.TransactionType.PAYMENT)
                .amount(new BigDecimal("100.00"))
                .currency("ILS")
                .status(Transaction.TransactionStatus.PENDING)
                .retryCount(0)
                .build();
    }

    private TransactionRule passingRule(String name) {
        TransactionRule rule = mock(TransactionRule.class);
        lenient().when(rule.getRuleName()).thenReturn(name);
        lenient().when(rule.evaluate(any())).thenReturn(RuleResult.pass(name));
        return rule;
    }

    private TransactionRule failingRule(String name, String reason) {
        TransactionRule rule = mock(TransactionRule.class);
        lenient().when(rule.getRuleName()).thenReturn(name);
        lenient().when(rule.evaluate(any())).thenReturn(RuleResult.fail(name, reason));
        return rule;
    }
    // --- tests ---

    @Test
    @DisplayName("Should pass when all rules pass")
    void shouldPassWhenAllRulesPass() {
        RuleEngine engine = new RuleEngine(List.of(
                passingRule("RULE_A"),
                passingRule("RULE_B"),
                passingRule("RULE_C")
        ));

        RuleResult result = engine.evaluate(buildTransaction());

        assertThat(result.passed()).isTrue();
        assertThat(result.ruleName()).isEqualTo("ALL_RULES");
    }

    @Test
    @DisplayName("Should fail on first failing rule and stop")
    void shouldFailFastOnFirstFailingRule() {
        TransactionRule failingRule = failingRule("RULE_B", "something is wrong");
        TransactionRule thirdRule   = passingRule("RULE_C");

        RuleEngine engine = new RuleEngine(List.of(
                passingRule("RULE_A"),
                failingRule,
                thirdRule
        ));

        RuleResult result = engine.evaluate(buildTransaction());

        assertThat(result.passed()).isFalse();
        assertThat(result.ruleName()).isEqualTo("RULE_B");
        assertThat(result.reason()).isEqualTo("something is wrong");

        // RULE_C must never have been called — fail-fast proof
        verify(thirdRule, never()).evaluate(any());
    }

    @Test
    @DisplayName("Should fail when first rule fails — no other rules run")
    void shouldFailImmediatelyWhenFirstRuleFails() {
        TransactionRule secondRule = passingRule("RULE_B");

        RuleEngine engine = new RuleEngine(List.of(
                failingRule("RULE_A", "first rule failed"),
                secondRule
        ));

        engine.evaluate(buildTransaction());

        verify(secondRule, never()).evaluate(any());
    }

    @Test
    @DisplayName("Should pass with empty rule list")
    void shouldPassWithNoRules() {
        RuleEngine engine = new RuleEngine(List.of());

        RuleResult result = engine.evaluate(buildTransaction());

        assertThat(result.passed()).isTrue();
    }

    @Test
    @DisplayName("Should return correct failing rule name and reason")
    void shouldReturnCorrectFailureDetails() {
        RuleEngine engine = new RuleEngine(List.of(
                failingRule("AMOUNT_VALIDATION", "Amount must be positive, got: -50")
        ));

        RuleResult result = engine.evaluate(buildTransaction());

        assertThat(result.passed()).isFalse();
        assertThat(result.ruleName()).isEqualTo("AMOUNT_VALIDATION");
        assertThat(result.reason()).contains("positive");
    }

    @Test
    @DisplayName("Should evaluate all rules when all pass")
    void shouldEvaluateAllRulesWhenAllPass() {
        TransactionRule ruleA = passingRule("RULE_A");
        TransactionRule ruleB = passingRule("RULE_B");
        TransactionRule ruleC = passingRule("RULE_C");

        RuleEngine engine = new RuleEngine(List.of(ruleA, ruleB, ruleC));
        Transaction tx = buildTransaction();

        engine.evaluate(tx);

        // All three rules must have been called exactly once
        verify(ruleA, times(1)).evaluate(tx);
        verify(ruleB, times(1)).evaluate(tx);
        verify(ruleC, times(1)).evaluate(tx);
    }
}