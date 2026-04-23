package com.finpipeline.etl;

import com.finpipeline.domain.Transaction;
import com.finpipeline.domain.Transaction.SourceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * ETL Transformer — enrichment and normalization stage.
 *
 * Runs AFTER the RuleEngine (data is already validated) and BEFORE persistence.
 * Each source system gets its own normalization branch — this is where schema
 * mapping lives when different upstream systems send data in different shapes.
 *
 * Mutates the transaction in-place intentionally: the transaction object is the
 * pipeline's unit of work, and cloning it at every stage would be wasteful here.
 */
@Service
public class ETLTransformer {

    private static final Logger log = LoggerFactory.getLogger(ETLTransformer.class);

    // All internal amounts are stored in 2 decimal places (cents-compatible)
    private static final int INTERNAL_SCALE = 2;

    /**
     * Entry point — routes to the correct normalization strategy per source system.
     */
    public Transaction transform(Transaction transaction) {
        log.debug("Transforming transaction id={}, source={}",
                transaction.getId(), transaction.getSourceSystem());

        normalizeAmount(transaction);
        normalizeCurrency(transaction);
        enrichMetadata(transaction);
        applySourceSystemMapping(transaction);

        log.debug("Transformation complete for id={}", transaction.getId());
        return transaction;
    }

    /**
     * Enforce consistent decimal scale across all amounts.
     * Prevents subtle comparison bugs downstream (e.g. 10.1 != 10.10 in some contexts).
     */
    private void normalizeAmount(Transaction transaction) {
        if (transaction.getAmount() != null) {
            BigDecimal normalized = transaction.getAmount()
                    .setScale(INTERNAL_SCALE, RoundingMode.HALF_UP);
            transaction.setAmount(normalized);
        }
    }

    /**
     * Uppercase and trim — defensive against upstream systems sending "usd" or " EUR ".
     */
    private void normalizeCurrency(Transaction transaction) {
        if (transaction.getCurrency() != null) {
            transaction.setCurrency(
                    transaction.getCurrency().toUpperCase().trim()
            );
        }
    }

    /**
     * Stamp the processing time. receivedAt was set at ingest —
     * processedAt marks when the pipeline touched it.
     */
    private void enrichMetadata(Transaction transaction) {
        transaction.setProcessedAt(LocalDateTime.from(Instant.now()));
    }

    /**
     * Source-system-specific schema mapping.
     * In a real system each branch would handle field renaming, unit conversion,
     * timezone normalization, etc. from that system's canonical format.
     */
    private void applySourceSystemMapping(Transaction transaction) {
        if (transaction.getSourceSystem() == null) return;

        switch (transaction.getSourceSystem()) {
            case TAX_AUTHORITY -> applyTaxAuthorityMapping(transaction);
            case BANK_API      -> applyBankApiMapping(transaction);
            case PAYMENT_GATEWAY -> applyPaymentGatewayMapping(transaction);
        }
    }

    /**
     * TAX_AUTHORITY — amounts arrive in agorot (1/100 ILS), convert to ILS.
     * Also enforces ILS as the stored currency regardless of what was sent.
     */
    private void applyTaxAuthorityMapping(Transaction transaction) {
        log.debug("Applying TAX_AUTHORITY mapping for id={}", transaction.getId());

        if (transaction.getAmount() != null) {
            BigDecimal converted = transaction.getAmount()
                    .divide(BigDecimal.valueOf(100), INTERNAL_SCALE, RoundingMode.HALF_UP);
            transaction.setAmount(converted);
        }
        transaction.setCurrency("ILS");
    }

    /**
     * BANK_API — amounts are already in major currency units, no conversion needed.
     * Strip any trailing whitespace from currency (known upstream quirk).
     */
    private void applyBankApiMapping(Transaction transaction) {
        log.debug("Applying BANK_API mapping for id={}", transaction.getId());
        // Amount is already in correct units — normalization above is sufficient
        // Placeholder for future field mappings (e.g. reference number remapping)
    }

    /**
     * PAYMENT_GATEWAY — amounts arrive in the minor unit of the currency (cents, pence, etc.).
     * Convert to major unit: divide by 100.
     */
    private void applyPaymentGatewayMapping(Transaction transaction) {
        log.debug("Applying PAYMENT_GATEWAY mapping for id={}", transaction.getId());

        if (transaction.getAmount() != null) {
            BigDecimal converted = transaction.getAmount()
                    .divide(BigDecimal.valueOf(100), INTERNAL_SCALE, RoundingMode.HALF_UP);
            transaction.setAmount(converted);
        }
    }
}