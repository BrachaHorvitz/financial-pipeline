package com.finpipeline.api;

import com.finpipeline.broker.TransactionEventPublisher;
import com.finpipeline.domain.Transaction;
import com.finpipeline.repository.TransactionRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Valid;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import com.finpipeline.generator.TransactionDataGenerator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    private final TransactionEventPublisher publisher;
    private final TransactionRepository repository;
    private final TransactionDataGenerator generator;
    private final Validator validator;

    @PostMapping
    public Mono<ResponseEntity<String>> ingest(@RequestBody @Valid TransactionRequest request) {
        return Mono.fromCallable(() -> ingestOne(request))
                .map(result -> switch (result.status()) {
                    case ACCEPTED -> ResponseEntity.accepted().body(result.message());
                    case DUPLICATE -> ResponseEntity.status(HttpStatus.CONFLICT).body(result.message());
                    case INVALID -> ResponseEntity.badRequest().body(result.message());
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Each item is validated and processed independently; the response carries one
     * machine-readable result per item (same order as the request is not guaranteed).
     */
    @PostMapping("/batch")
    public Flux<BatchItemResult> ingestBatch(@RequestBody List<TransactionRequest> requests) {
        return Flux.fromIterable(requests)
                .flatMap(req -> Mono.fromCallable(() -> ingestOne(req))
                        .subscribeOn(Schedulers.boundedElastic()));
    }

    /**
     * Shared ingest path for single and batch requests: validation, duplicate check, save,
     * async publish. The pre-check covers the common case; the unique constraint on
     * deduplicationKey is the real guard, so a concurrent insert of the same key also ends
     * up as DUPLICATE.
     */
    private BatchItemResult ingestOne(TransactionRequest request) {
        String key = request == null ? null : request.deduplicationKey();
        if (request == null) {
            return BatchItemResult.invalid(null, "Request body is empty");
        }

        Set<ConstraintViolation<TransactionRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            return BatchItemResult.invalid(key, message);
        }

        Transaction.SourceSystem source;
        Transaction.TransactionType type;
        try {
            source = Transaction.SourceSystem.valueOf(request.sourceSystem());
            type = Transaction.TransactionType.valueOf(request.transactionType());
        } catch (IllegalArgumentException e) {
            return BatchItemResult.invalid(key, "Unknown sourceSystem or transactionType");
        }

        if (repository.findByDeduplicationKey(key).isPresent()) {
            return duplicate(key);
        }

        Transaction tx = Transaction.builder()
                .deduplicationKey(key)
                .sourceSystem(source)
                .transactionType(type)
                .amount(request.amount())
                .currency(request.currency())
                .status(Transaction.TransactionStatus.PENDING)
                .rawPayload(request.rawPayload())
                .retryCount(0)
                .build();

        Transaction saved;
        try {
            saved = repository.save(tx);
        } catch (DataIntegrityViolationException e) {
            // Only a unique-key collision is a duplicate; other constraint violations are real errors.
            if (repository.findByDeduplicationKey(key).isPresent()) {
                return duplicate(key);
            }
            throw e;
        }
        publisher.publishAsync(saved);
        return BatchItemResult.accepted(key, saved.getId());
    }

    private BatchItemResult duplicate(String deduplicationKey) {
        log.info("Duplicate deduplicationKey detected at ingest — key={}", deduplicationKey);
        return BatchItemResult.duplicate(deduplicationKey);
    }

    @GetMapping("/{id}")
    public Mono<Transaction> getById(@PathVariable String id) {
        return Mono.fromCallable(() -> repository.findById(id)
                        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/generate")
    public Mono<ResponseEntity<String>> generateOne() {
        return Mono.fromCallable(() -> {
            Transaction tx = generator.generateAndPublish();
            return ResponseEntity.accepted()
                    .body("Generated: " + tx.getId());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/generate/batch/{count}")
    public Mono<ResponseEntity<String>> generateBatch(@PathVariable int count) {
        return Mono.fromCallable(() -> {
            generator.generateBatch(count);
            return ResponseEntity.accepted()
                    .body("Generated batch of: " + count);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}