# Financial Transaction Processing Pipeline

A compact, highly technical Spring Boot portfolio project demonstrating advanced backend engineering — real-time ETL processing, event-driven architecture, idempotent processing, and message brokering.

> Inspired by real-world patterns from enterprise financial integrations: connecting government tax APIs, bank systems, and payment gateways with reliability, deduplication, and failure recovery built in.

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Framework | Spring Boot 3.3 + WebFlux | Reactive, non-blocking |
| Message Broker | RabbitMQ | Exchange, queue, Dead Letter Queue |
| Database | H2 in-memory + Spring Data JPA | Zero setup, runs anywhere |
| Serialization | Jackson JSON | Messages stored as readable JSON |
| Validation | Bean Validation | `@NotBlank`, `@Positive`, `@NotNull` |
| Build | Maven | `./mvnw spring-boot:run` |
| Testing | JUnit 5 + StepVerifier | Reactive test support |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ingest Layer                             │
│   REST API (WebFlux)  ──►  H2 Database  ──►  RabbitMQ Queue    │
│   POST /transactions        PENDING              publisher       │
└─────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Processing Layer                           │
│   @RabbitListener  ──►  Rule Engine  ──►  ETL Transformer       │
│   TransactionConsumer    Strategy Pattern   enrichment/mapping  │
│                               │                                 │
│                               ▼                                 │
│                    Idempotent Processor                         │
│                    dedup key + retry logic + DLQ routing        │
└─────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Reconciliation Layer                        │
│   @Scheduled Job  ──►  Multi-stage SQL matching  ──►  Reports   │
│                              indexing               CSV + JSON  │
└─────────────────────────────────────────────────────────────────┘
```

### Full Data Flow

```
REST API → H2 save → RabbitMQ → Rule Engine → ETL Transformer → Idempotency → Reconciliation → Reports
  ✅          ✅          ✅          🔜               🔜               🔜             🔜            🔜
```

---

## Key Design Patterns

- **Event-driven architecture** — REST layer is fully decoupled from processing via RabbitMQ
- **Strategy Pattern** — pluggable rule engine; each source system has its own validation rules
- **Idempotent processing** — `deduplicationKey` unique constraint prevents double-processing
- **Dead Letter Queue** — failed messages are routed to DLQ after max retries; never lost
- **Reactive API** — WebFlux with `Mono`/`Flux`; blocking JPA calls offloaded to `boundedElastic` scheduler
- **Builder Pattern** — Lombok `@Builder` for clean, readable entity construction

---

## Running Locally

### Prerequisites

- Java 17+ (Amazon Corretto recommended)
- Maven (included via `mvnw` wrapper)
- RabbitMQ installed locally ([rabbitmq.com](https://www.rabbitmq.com/install-windows.html))

### Start RabbitMQ

```bash
# Enable management UI (run once, as Administrator)
rabbitmq-plugins enable rabbitmq_management
```

RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672) — login: `guest` / `guest`

### Run the application

```bash
./mvnw spring-boot:run
```

H2 Console: [http://localhost:8080/h2-console](http://localhost:8080/h2-console)
JDBC URL: `jdbc:h2:mem:finpipeline`

### Generate test data
```
POST http://localhost:8080/api/v1/transactions/generate/batch/10
```
### ⚠️ Queue purge required on restart

This project uses H2 (in-memory database). All data is lost on every app restart.
RabbitMQ queues are durable — messages persist across restarts.

**On every restart:** purge both queues before generating new data, otherwise
the consumer will attempt to process messages referencing IDs that no longer
exist in H2.

Purge via RabbitMQ Management UI → Queues → Purge, or via CLI:
```bash
rabbitmqadmin purge queue name=transaction.queue
rabbitmqadmin purge queue name=transaction.dlq
```
In production this constraint disappears — a persistent database (PostgreSQL, etc.)
retains data across restarts and no purge is needed.

### JPA + Message Broker: detached entity pattern

Entities deserialized from RabbitMQ JSON are **detached** from the JPA session.
The `IdempotentProcessor` reloads each entity by ID from the database before
mutating and saving it. This is the correct pattern for any pipeline where JPA
entities travel as broker messages — bypassing it causes `StaleObjectStateException`.
---

## API Endpoints

### Ingest a single transaction

```bash
POST /api/v1/transactions
Content-Type: application/json

{
  "deduplicationKey": "tx-001",
  "sourceSystem": "BANK_API",
  "transactionType": "PAYMENT",
  "amount": 1500.00,
  "currency": "ILS",
  "rawPayload": "{\"ref\": \"bank-ref-001\"}"
}
```

Response: `202 Accepted` — transaction queued asynchronously.

### Ingest a batch

```bash
POST /api/v1/transactions/batch
Content-Type: application/json

[ { ... }, { ... } ]
```

### Generate mock data

```bash
# Single transaction
POST /api/v1/transactions/generate

# Batch of N transactions
POST /api/v1/transactions/generate/batch/{count}
```

### Get transaction by ID

```bash
GET /api/v1/transactions/{id}
```

---

## Project Structure

```
src/main/java/com/finpipeline/
├── api/
│   ├── TransactionController.java      # WebFlux REST endpoints
│   └── TransactionRequest.java         # DTO record with validation
├── broker/
│   ├── TransactionEventPublisher.java  # Publishes to RabbitMQ exchange
│   └── RabbitMQConfig.java             # Queue, DLQ, exchange, bindings
├── config/
├── domain/
│   └── Transaction.java                # JPA entity
├── repository/
│   └── TransactionRepository.java      # Spring Data JPA queries
├── generator/
│   └── TransactionDataGenerator.java   # Mock data producer
├── rules/                              # 🔜 Day 2 — Rule Engine
├── etl/                                # 🔜 Day 2 — ETL Transformer
├── processor/                          # 🔜 Day 2 — Consumer + Idempotency
└── reconciliation/                     # 🔜 Day 3 — Scheduled job + reports
```

---

## Domain Model

### Transaction entity

| Field | Type | Notes |
|---|---|---|
| `id` | UUID | Auto-generated primary key |
| `deduplicationKey` | String | Unique constraint — idempotency key |
| `sourceSystem` | String | `TAX_AUTHORITY`, `BANK_API`, `PAYMENT_GATEWAY` |
| `transactionType` | String | `PAYMENT`, `REFUND`, `TRANSFER` |
| `amount` | BigDecimal | Exact decimal — never `double` for money |
| `currency` | String | `ILS`, `USD`, `EUR` |
| `status` | String | `PENDING`, `PROCESSED`, `FAILED`, `DUPLICATE` |
| `retryCount` | Integer | Incremented on processing failure |
| `receivedAt` | LocalDateTime | Auto-set by `@CreationTimestamp` |
| `processedAt` | LocalDateTime | Set when processing completes |

### RabbitMQ topology

| Name | Type | Purpose |
|---|---|---|
| `transaction.exchange` | DirectExchange | Routes incoming messages |
| `transaction.queue` | Durable queue | Main processing queue |
| `transaction.dlx` | DirectExchange | Dead letter exchange |
| `transaction.dlq` | Durable queue | Failed messages after max retries |

Routing key: `transaction.received`
DLQ routing key: `transaction.failed`

---

## Why 202 Accepted?

The API returns `202 Accepted`, not `200 OK`. This is intentional:

- `200 OK` — request was processed synchronously and is complete
- `202 Accepted` — request was received and is being processed asynchronously

The transaction is saved to H2 and published to RabbitMQ before returning. The actual business processing (rule validation, ETL, idempotency check) happens in the consumer — asynchronously, after the HTTP response is already sent.

---

## Why BigDecimal for amounts?

```java
// Never do this with money:
double a = 0.1 + 0.2;  // = 0.30000000000000004

// Always use BigDecimal:
BigDecimal amount = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
```

Floating-point arithmetic has precision errors that are unacceptable in financial systems.

---

## Roadmap

### Day 1 ✅ — Core Infrastructure
- Spring Boot + WebFlux setup
- Transaction JPA entity with deduplication key
- RabbitMQ config — main queue, DLQ, exchange, JSON serialization
- REST API — single ingest, batch ingest, mock generator
- Spring Data JPA repository

### Day 2 🔜 — Business Logic
- `TransactionConsumer` — `@RabbitListener` entry point
- `RuleEngine` — Strategy Pattern, pluggable rules per source system
- `ETLTransformer` — enrichment, schema normalization
- `IdempotentProcessor` — dedup key check, retry logic, DLQ routing
- Unit tests per rule and transformer

### Day 3 🔜 — Reconciliation & Output
- `@Scheduled` reconciliation job
- Multi-stage SQL matching with indexing
- CSV + JSON report generation
- Micrometer metrics + Actuator endpoints

---

## Git Log

```
feat: day 1 complete — WebFlux REST API, RabbitMQ broker, JPA entity, mock data generator
feat: add RabbitMQ config with main queue, DLQ, and JSON serialization
feat: initial Spring Boot project setup
```
