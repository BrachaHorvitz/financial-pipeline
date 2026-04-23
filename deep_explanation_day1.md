# Financial Transaction Pipeline — Deep Explanation
## Day 1: What We Built and Why

---

## Table of Contents

1. [Big Picture — What is this project?](#1-big-picture)
2. [The Data Flow](#2-the-data-flow)
3. [Tech Stack Decisions](#3-tech-stack-decisions)
4. [File-by-File Code Explanation](#4-file-by-file-code-explanation)
   - [Transaction.java](#41-transactionjava)
   - [RabbitMQConfig.java](#42-rabbitmqconfigjava)
   - [TransactionEventPublisher.java](#43-transactioneventpublisherjava)
   - [TransactionRequest.java](#44-transactionrequestjava)
   - [TransactionRepository.java](#45-transactionrepositoryjava)
   - [TransactionController.java](#46-transactioncontrollerjava)
   - [TransactionDataGenerator.java](#47-transactiondatageneratorjava)
5. [Key Concepts Explained](#5-key-concepts-explained)
6. [What Day 2 Will Add](#6-what-day-2-will-add)

---

## 1. Big Picture

This project simulates a **real-world financial transaction processing pipeline** — the kind that exists in banks, payment gateways, and tax systems. The core problem it solves:

> "Messages arrive from multiple external systems, in different formats, at unpredictable times. We need to validate, transform, deduplicate, and persist them reliably — without losing any data, even if something crashes."

This is exactly what you built at Intentia Israel — and this project is a clean, portfolio-grade demonstration of those same patterns.

### The three hard problems it solves:

| Problem | Solution |
|---|---|
| Messages arrive from multiple unreliable sources | RabbitMQ broker decouples producers from consumers |
| Same transaction can arrive twice (network retry) | Idempotency via `deduplicationKey` (Day 2) |
| Processing can fail mid-way | Dead Letter Queue + retry logic (Day 2) |

---

## 2. The Data Flow

```
[External System / Generator]
         │
         ▼
[REST API — TransactionController]   ← HTTP POST /api/v1/transactions
         │
         ▼
[H2 Database — save as PENDING]      ← JPA / TransactionRepository
         │
         ▼
[RabbitMQ — transaction.exchange]    ← TransactionEventPublisher
         │
         ▼
[transaction.queue]                  ← messages waiting here (12 messages now)
         │
         ▼ ← Day 2 starts here
[TransactionConsumer — @RabbitListener]
         │
         ├──► Rule Engine (validate + filter)
         │
         ├──► ETL Transformer (enrich + normalize)
         │
         ├──► Idempotent Processor (dedup + retry)
         │
         ▼
[H2 Database — update status to PROCESSED / FAILED / DUPLICATE]
         │
         ▼ ← Day 3
[Reconciliation Job — @Scheduled]
         │
         ▼
[CSV + JSON Reports]
```

### Why is RabbitMQ in the middle?

Without RabbitMQ, the flow would be synchronous:

```
HTTP Request → validate → transform → save → HTTP Response
```

This is fragile. If the transformation step crashes, the message is lost. The HTTP client times out. The user gets an error.

With RabbitMQ:

```
HTTP Request → save to DB → publish to queue → HTTP Response (202 Accepted)
                                    ↓
                            consumer picks it up separately
```

The HTTP response is immediate. The processing happens asynchronously. If the consumer crashes, the message stays in the queue and gets reprocessed when it comes back up.

---

## 3. Tech Stack Decisions

### Spring Boot 3.x + WebFlux (Reactive)

WebFlux is Spring's reactive web framework. Instead of the traditional blocking model (one thread per request), WebFlux uses **non-blocking I/O** — a small number of threads handle thousands of concurrent requests.

**Why it matters here:** Financial systems handle high volumes of concurrent transactions. Blocking I/O would exhaust the thread pool under load. WebFlux handles this efficiently.

Key types you'll see:
- `Mono<T>` — a stream that emits 0 or 1 item (like `Optional` but async)
- `Flux<T>` — a stream that emits 0 to N items (like `List` but async)
- `Schedulers.boundedElastic()` — a thread pool for blocking operations (like DB calls) inside a reactive context

### RabbitMQ

RabbitMQ is a **message broker** — a middleman that receives messages from producers and delivers them to consumers. Key concepts:

- **Exchange** — receives messages and routes them to queues based on routing keys
- **Queue** — stores messages until a consumer picks them up
- **Routing key** — a label on the message that the exchange uses to decide which queue to send it to
- **Dead Letter Queue (DLQ)** — a special queue where messages go when they fail processing too many times

### H2 In-Memory Database

H2 is a Java database that runs inside the JVM — no installation needed. Data is lost when the app stops, which is fine for a portfolio project. In production this would be PostgreSQL or SQL Server.

### Jackson JSON

Jackson converts Java objects to JSON and back. Used here to serialize `Transaction` objects into JSON messages that RabbitMQ stores in the queue.

---

## 4. File-by-File Code Explanation

---

### 4.1 `Transaction.java`

**Package:** `domain/`
**Role:** The core data model — represents one financial transaction in the system.

```java
@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
```

**Annotations explained:**

| Annotation | What it does |
|---|---|
| `@Entity` | Tells JPA this class maps to a database table named `transaction` |
| `@Data` | Lombok — auto-generates `getters`, `setters`, `equals`, `hashCode`, `toString` |
| `@Builder` | Lombok — enables the builder pattern: `Transaction.builder().amount(...).build()` |
| `@NoArgsConstructor` | Lombok — generates empty constructor (required by JPA) |
| `@AllArgsConstructor` | Lombok — generates constructor with all fields (required by `@Builder`) |

```java
@Id
@GeneratedValue(strategy = GenerationType.UUID)
private String id;
```

Every transaction gets a unique UUID as its primary key. Generated automatically by JPA on save.

```java
@Column(nullable = false, unique = true)
private String deduplicationKey;
```

This is the **idempotency key**. If the same transaction arrives twice (network retry, duplicate submission), this unique constraint prevents it from being saved twice. The second attempt will get a database constraint violation, which we catch and mark as `DUPLICATE`. This is one of the most important fields in the whole system.

```java
private String sourceSystem;      // TAX_AUTHORITY, BANK_API, PAYMENT_GATEWAY
private String transactionType;   // PAYMENT, REFUND, TRANSFER
private String status;            // PENDING, PROCESSED, FAILED, DUPLICATE
private Integer retryCount;
```

- `sourceSystem` — which external system sent this transaction. Important because each system has different validation rules (Day 2 Rule Engine).
- `status` — tracks where the transaction is in the pipeline lifecycle.
- `retryCount` — how many times processing has been attempted. When it exceeds the max, the message goes to the DLQ.

```java
@CreationTimestamp
private LocalDateTime receivedAt;
private LocalDateTime processedAt;
```

`@CreationTimestamp` — automatically set by Hibernate when the record is saved. `processedAt` is set manually when processing completes.

---

### 4.2 `RabbitMQConfig.java`

**Package:** `config/`
**Role:** Declares and wires all RabbitMQ infrastructure — queues, exchanges, bindings, and message serialization.

```java
public static final String TRANSACTION_QUEUE    = "transaction.queue";
public static final String DEAD_LETTER_QUEUE    = "transaction.dlq";
public static final String TRANSACTION_EXCHANGE = "transaction.exchange";
public static final String DEAD_LETTER_EXCHANGE = "transaction.dlx";
public static final String ROUTING_KEY          = "transaction.received";
public static final String DLQ_ROUTING_KEY      = "transaction.failed";
```

Constants are `public static final` so other classes can reference them without hardcoding strings. This prevents typos and makes refactoring easy.

#### The Dead Letter Queue setup

```java
@Bean
public DirectExchange deadLetterExchange() {
    return new DirectExchange(DEAD_LETTER_EXCHANGE);
}

@Bean
public Queue deadLetterQueue() {
    return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
}

@Bean
public Binding deadLetterBinding() {
    return BindingBuilder
        .bind(deadLetterQueue())
        .to(deadLetterExchange())
        .with(DLQ_ROUTING_KEY);
}
```

The DLQ is set up **first**, because the main queue references it. This is the "safety net" — messages that fail processing get routed here automatically by RabbitMQ.

#### The main queue — with DLQ wired in

```java
@Bean
public Queue transactionQueue() {
    return QueueBuilder.durable(TRANSACTION_QUEUE)
        .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
        .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
        .build();
}
```

`x-dead-letter-exchange` and `x-dead-letter-routing-key` are **RabbitMQ queue arguments** — they tell RabbitMQ: "if a message in this queue is rejected or expired, send it to this exchange with this routing key." This is RabbitMQ's native dead-lettering mechanism.

`durable(...)` means the queue survives a RabbitMQ restart. Messages are persisted to disk.

#### How a message travels:

```
Publisher
   │ convertAndSend(TRANSACTION_EXCHANGE, "transaction.received", transaction)
   ▼
transaction.exchange  ← DirectExchange routes by routing key
   │ routing key = "transaction.received"
   ▼
transaction.queue  ← message sits here
   │ (if rejected/failed)
   ▼
transaction.dlx  ← dead letter exchange
   │ routing key = "transaction.failed"
   ▼
transaction.dlq  ← permanently failed messages
```

#### JSON serialization

```java
@Bean
public Jackson2JsonMessageConverter messageConverter() {
    return new Jackson2JsonMessageConverter();
}

@Bean
public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
    RabbitTemplate template = new RabbitTemplate(connectionFactory);
    template.setMessageConverter(messageConverter());
    return template;
}
```

By default RabbitMQ stores messages as raw bytes. We configure Jackson so messages are stored as readable JSON. This means you can open the RabbitMQ Management UI, click on a message, and read it as JSON — invaluable for debugging.

---

### 4.3 `TransactionEventPublisher.java`

**Package:** `broker/`
**Role:** Single responsibility — publish a `Transaction` to RabbitMQ. Nothing else.

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventPublisher {

    private final RabbitTemplate rabbitTemplate;

    public void publish(Transaction transaction) {
        log.info("[BROKER] Publishing transaction {} from {} to queue",
            transaction.getId(), transaction.getSourceSystem());

        rabbitTemplate.convertAndSend(
            RabbitMQConfig.TRANSACTION_EXCHANGE,
            RabbitMQConfig.ROUTING_KEY,
            transaction
        );
    }
}
```

**Why a dedicated publisher class?**

The controller could call `rabbitTemplate.convertAndSend(...)` directly. But wrapping it in a publisher gives you:

- One place to add logging, metrics, or error handling around publishing
- Easy to mock in unit tests
- Clear separation of concerns — the controller handles HTTP, the publisher handles messaging

`@RequiredArgsConstructor` — Lombok generates a constructor that injects `RabbitTemplate`. This is constructor injection, which is preferred over `@Autowired` field injection because it makes dependencies explicit and enables immutability.

`convertAndSend(exchange, routingKey, object)` — Jackson serializes `transaction` to JSON, RabbitMQ routes it via the exchange to the queue.

---

### 4.4 `TransactionRequest.java`

**Package:** `api/`
**Role:** Data Transfer Object (DTO) — the shape of the HTTP request body.

```java
public record TransactionRequest(
    @NotBlank String deduplicationKey,
    @NotBlank String sourceSystem,
    @NotBlank String transactionType,
    @NotNull @Positive BigDecimal amount,
    @NotBlank String currency,
    String rawPayload
) {}
```

**Why a `record` and not a class?**

Java `record` (Java 16+) is an immutable data carrier. It auto-generates:
- Constructor with all fields
- Getters (accessed as `request.amount()`, not `request.getAmount()`)
- `equals()`, `hashCode()`, `toString()`

Perfect for DTOs because request objects should never be mutated after construction.

**Why separate from `Transaction` entity?**

The DTO and entity serve different purposes:
- `TransactionRequest` — what the client sends over HTTP. May contain raw/untrusted data.
- `Transaction` — what gets saved to the database. Has server-generated fields like `id`, `status`, `receivedAt`, `retryCount`.

Never expose your entity directly to the API layer — it couples your database schema to your API contract.

**Validation annotations:**

| Annotation | Meaning |
|---|---|
| `@NotBlank` | Must not be null, empty, or whitespace-only |
| `@NotNull` | Must not be null (but can be empty for Strings) |
| `@Positive` | Must be > 0 (rejects negative amounts and zero) |

If validation fails, Spring returns a `400 Bad Request` automatically before the controller method is even called.

---

### 4.5 `TransactionRepository.java`

**Package:** `repository/`
**Role:** Database access layer for `Transaction` entities.

```java
public interface TransactionRepository extends JpaRepository<Transaction, String> {
    Optional<Transaction> findByDeduplicationKey(String deduplicationKey);
    List<Transaction> findByStatus(String status);
    List<Transaction> findByStatusAndRetryCountLessThan(String status, int maxRetries);
}
```

`JpaRepository<Transaction, String>` — Spring Data JPA generates all standard CRUD operations automatically. The `String` is the type of the primary key.

**The custom queries** are generated automatically by Spring Data JPA from the method name — no SQL needed:

| Method | Generated SQL equivalent |
|---|---|
| `findByDeduplicationKey(key)` | `SELECT * FROM transaction WHERE deduplication_key = ?` |
| `findByStatus(status)` | `SELECT * FROM transaction WHERE status = ?` |
| `findByStatusAndRetryCountLessThan(status, max)` | `SELECT * FROM transaction WHERE status = ? AND retry_count < ?` |

`findByStatusAndRetryCountLessThan` will be used in Day 2 by the retry logic — "find all FAILED transactions that haven't exceeded the max retry count."

---

### 4.6 `TransactionController.java`

**Package:** `api/`
**Role:** HTTP entry point — receives requests, saves to DB, publishes to RabbitMQ.

#### Single transaction endpoint

```java
@PostMapping
public Mono<ResponseEntity<String>> ingest(@RequestBody @Valid TransactionRequest request) {
    return Mono.fromCallable(() -> {
        Transaction tx = Transaction.builder()
            .deduplicationKey(request.deduplicationKey())
            ...
            .status("PENDING")
            .retryCount(0)
            .build();

        Transaction saved = repository.save(tx);
        publisher.publish(saved);
        return ResponseEntity.accepted().body("Transaction queued: " + saved.getId());
    }).subscribeOn(Schedulers.boundedElastic());
}
```

**`Mono.fromCallable(...)`** — wraps a blocking operation (the JPA `repository.save()` call) in a `Mono`. JPA is blocking — it waits for the database to respond. In a reactive context, blocking is forbidden on the event loop thread, so we offload it.

**`.subscribeOn(Schedulers.boundedElastic())`** — executes the blocking lambda on a dedicated thread pool designed for blocking I/O. This keeps the reactive event loop thread free for non-blocking work.

**`ResponseEntity.accepted()`** — returns HTTP 202 (Accepted), not 200 (OK). The distinction is important: 202 means "we received your request and it is being processed asynchronously." The transaction is in the queue but not yet processed. This is the correct semantic for async processing.

**`@Valid`** — triggers Bean Validation on the `TransactionRequest`. Without it, validation annotations are ignored.

#### Batch endpoint

```java
@PostMapping("/batch")
public Flux<String> ingestBatch(@RequestBody List<TransactionRequest> requests) {
    return Flux.fromIterable(requests)
        .flatMap(req -> Mono.fromCallable(() -> {
            // save + publish each transaction
            return saved.getId();
        }).subscribeOn(Schedulers.boundedElastic()));
}
```

`Flux.fromIterable(requests)` — creates a reactive stream from the list.

`.flatMap(...)` — for each request, runs the save+publish operation concurrently (not sequentially). This is more efficient than processing them one by one. `flatMap` subscribes to all inner `Mono`s concurrently and merges the results.

#### Status endpoint

```java
@GetMapping("/{id}")
public Mono<Transaction> getById(@PathVariable String id) {
    return Mono.fromCallable(() -> repository.findById(id)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND)))
        .subscribeOn(Schedulers.boundedElastic());
}
```

`ResponseStatusException(HttpStatus.NOT_FOUND)` — Spring WebFlux automatically converts this to a 404 HTTP response with a proper error body.

---

### 4.7 `TransactionDataGenerator.java`

**Package:** `generator/`
**Role:** Generates realistic mock transactions for testing — simulates external systems sending data.

```java
private static final String[] TYPES   = {"PAYMENT", "REFUND", "TRANSFER"};
private static final String[] SYSTEMS = {"TAX_AUTHORITY", "BANK_API", "PAYMENT_GATEWAY"};
private static final String[] CURRENCIES = {"ILS", "USD", "EUR"};
```

Static arrays of valid values. `random.nextInt(SYSTEMS.length)` picks a random index.

```java
public Transaction generateAndPublish() {
    Transaction tx = Transaction.builder()
        .deduplicationKey(UUID.randomUUID().toString())
        .amount(BigDecimal.valueOf(random.nextDouble() * 10000)
            .setScale(2, RoundingMode.HALF_UP))
        ...
        .build();

    Transaction saved = repository.save(tx);
    publisher.publish(saved);
    return saved;
}
```

`UUID.randomUUID()` — guarantees every generated transaction has a unique deduplication key.

`BigDecimal.valueOf(...).setScale(2, RoundingMode.HALF_UP)` — never use `double` for money. Floating-point arithmetic has precision errors (e.g. `0.1 + 0.2 = 0.30000000000000004`). `BigDecimal` is exact. `setScale(2, HALF_UP)` rounds to 2 decimal places.

```java
public void generateBatch(int count) {
    for (int i = 0; i < count; i++) {
        generateAndPublish();
    }
}
```

Simple loop. In the controller we wrapped this in `Mono.fromCallable + subscribeOn(boundedElastic)` to keep it off the reactive event loop thread.

---

## 5. Key Concepts Explained

### Dependency Injection

Throughout the code, dependencies are injected via constructors:

```java
@RequiredArgsConstructor
public class TransactionController {
    private final TransactionEventPublisher publisher;
    private final TransactionRepository repository;
    private final TransactionDataGenerator generator;
}
```

Spring manages the lifecycle of all `@Component`, `@Service`, `@Repository`, and `@RestController` beans. When `TransactionController` is created, Spring automatically passes the required dependencies in. You never call `new TransactionController(...)` yourself.

This is **Inversion of Control (IoC)** — the framework controls object creation and wiring, not your code.

### Why `@Bean` in config classes?

`@Bean` methods in `@Configuration` classes tell Spring: "create this object and manage it as a bean." Spring ensures only one instance is created (singleton by default) and injects it wherever needed.

### AMQP vs HTTP

| | HTTP | AMQP (RabbitMQ) |
|---|---|---|
| Model | Request-Response | Publish-Subscribe |
| Coupling | Tight (caller waits) | Loose (fire and forget) |
| Durability | Lost if receiver down | Persisted in queue |
| Retry | Manual | Built-in |

### The Builder Pattern

```java
Transaction tx = Transaction.builder()
    .deduplicationKey("abc-123")
    .sourceSystem("BANK_API")
    .amount(new BigDecimal("1500.00"))
    .status("PENDING")
    .build();
```

Alternative to a long constructor call. Makes object construction readable and allows optional fields to be omitted. Lombok's `@Builder` generates this entire pattern automatically.

---

## 6. What Day 2 Will Add

Day 1 built the **ingest pipeline** — getting transactions into the system and into the queue.

Day 2 builds the **processing pipeline** — taking transactions out of the queue and doing real work:

### `TransactionConsumer` — `@RabbitListener`
Listens to `transaction.queue`. Every message that arrives triggers this method. This is the entry point to the processing pipeline.

### `RuleEngine` — Strategy Pattern
Each source system has different validation rules. `TAX_AUTHORITY` transactions might require a tax reference number. `PAYMENT_GATEWAY` transactions might need a card token. The Strategy Pattern lets us swap rules per source system without if/else chains.

### `ETLTransformer`
Different source systems send data in different formats. The transformer normalizes and enriches the data — adds calculated fields, maps codes to descriptions, fills defaults.

### `IdempotentProcessor`
Before processing, checks the `deduplicationKey` against the database. If it already exists and is `PROCESSED`, skip it and mark as `DUPLICATE`. If processing fails, increment `retryCount`. If `retryCount` exceeds the max, reject the message so RabbitMQ routes it to the DLQ.

```
Message arrives
     │
     ▼
Already in DB as PROCESSED? ──► YES ──► Mark DUPLICATE, ACK message
     │ NO
     ▼
Run Rule Engine ──► INVALID ──► Mark FAILED, increment retryCount
     │ VALID
     ▼
Run ETL Transformer
     │
     ▼
Save to DB as PROCESSED ──► ACK message (removed from queue)
     │
     └── retryCount >= max? ──► NACK message ──► RabbitMQ routes to DLQ
```
