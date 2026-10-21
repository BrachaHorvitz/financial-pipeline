package com.finpipeline.api;

import com.finpipeline.broker.TransactionEventPublisher;
import com.finpipeline.domain.Transaction;
import com.finpipeline.repository.TransactionRepository;
import jakarta.validation.Valid;
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

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    private final TransactionEventPublisher publisher;
    private final TransactionRepository repository;
    private final TransactionDataGenerator generator;

    @PostMapping
    public Mono<ResponseEntity<String>> ingest(@RequestBody @Valid TransactionRequest request) {
        return Mono.fromCallable(() -> ingestOne(request))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/batch")
    public Flux<String> ingestBatch(@RequestBody List<TransactionRequest> requests) {
        return Flux.fromIterable(requests)
                .flatMap(req -> Mono.fromCallable(() -> ingestOne(req))
                        .map(ResponseEntity::getBody)
                        .subscribeOn(Schedulers.boundedElastic()));
    }

    /**
     * Shared ingest path for single and batch requests: duplicate check, save, async publish.
     * The pre-check covers the common case; the unique constraint on deduplicationKey is the
     * real guard, so a concurrent insert of the same key also ends up as 409.
     */
    private ResponseEntity<String> ingestOne(TransactionRequest request) {
        if (repository.findByDeduplicationKey(request.deduplicationKey()).isPresent()) {
            return duplicateResponse(request.deduplicationKey());
        }

        Transaction tx = Transaction.builder()
                .deduplicationKey(request.deduplicationKey())
                .sourceSystem(Transaction.SourceSystem.valueOf(request.sourceSystem()))
                .transactionType(Transaction.TransactionType.valueOf(request.transactionType()))
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
            return duplicateResponse(request.deduplicationKey());
        }
        publisher.publishAsync(saved);
        return ResponseEntity.accepted().body("Transaction queued: " + saved.getId());
    }

    private ResponseEntity<String> duplicateResponse(String deduplicationKey) {
        log.info("Duplicate deduplicationKey detected at ingest — key={}", deduplicationKey);
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body("Duplicate transaction — key already exists: " + deduplicationKey);
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