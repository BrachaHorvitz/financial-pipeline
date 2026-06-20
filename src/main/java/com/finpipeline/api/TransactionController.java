package com.finpipeline.api;

import com.finpipeline.broker.TransactionEventPublisher;
import com.finpipeline.domain.Transaction;
import com.finpipeline.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
        return Mono.fromCallable(() -> {
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

            Transaction saved = repository.save(tx);
            publisher.publishAsync(saved); // ← שינוי מ-publish ל-publishAsync
            return ResponseEntity.accepted().body("Transaction queued: " + saved.getId());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/batch")
    public Flux<String> ingestBatch(@RequestBody List<TransactionRequest> requests) {
        return Flux.fromIterable(requests)
                .flatMap(req -> Mono.fromCallable(() -> {
                    Transaction tx = Transaction.builder()
                            .deduplicationKey(req.deduplicationKey())
                            .sourceSystem(Transaction.SourceSystem.valueOf(req.sourceSystem()))
                            .transactionType(Transaction.TransactionType.valueOf(req.transactionType()))
                            .amount(req.amount())
                            .currency(req.currency())
                            .status(Transaction.TransactionStatus.PENDING)
                            .retryCount(0)
                            .build();

                    Transaction saved = repository.save(tx);
                    publisher.publish(saved);
                    return saved.getId();
                }).subscribeOn(Schedulers.boundedElastic()));
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