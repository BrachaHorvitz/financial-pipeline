package com.finpipeline.repository;

import com.finpipeline.domain.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TransactionRepository extends JpaRepository<Transaction, String> {
    Optional<Transaction> findByDeduplicationKey(String deduplicationKey);
    List<Transaction> findByStatus(String status);
    List<Transaction> findByStatusAndRetryCountLessThan(String status, int maxRetries);
}