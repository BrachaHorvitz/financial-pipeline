# Financial Transaction Processing Pipeline

A compact, highly technical Spring Boot portfolio project demonstrating advanced backend engineering — real-time ETL processing, event-driven architecture, idempotent processing, and message brokering.

> Inspired by real-world patterns from enterprise financial integrations: connecting government tax APIs, bank systems, and payment gateways with reliability, deduplication, and failure recovery built in.

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Framework | Spring Boot 3.3 + WebFlux | Reactive, non-blocking |
| Message Broker | RabbitMQ | Exchange, queue, Dead Letter Queue |
| Database | H2 in-memory (local) / PostgreSQL (Docker) + Spring Data JPA | H2 for zero-setup local runs; PostgreSQL when running via Docker Compose |
| Serialization | Jackson JSON | Messages stored as readable JSON |
| Validation | Bean Validation | `@NotBlank`, `@Positive`, `@NotNull` |
| Build | Maven | `./mvnw spring-boot:run` |
| Testing | JUnit 5 + StepVerifier | Reactive test support |
| Containerization | Docker + Docker Compose | Multi-stage `Dockerfile`; Compose runs app + PostgreSQL + RabbitMQ |
| Cloud Deployment | AWS ECS (Fargate) | Task definition for running the containerized app on AWS — see [AWS_deploy.md](AWS_deploy.md) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ingest Layer                             │
│   REST API (WebFlux)  ──►  Database  ──►  RabbitMQ Queue       │
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
```

Database is H2 in-memory for local (`./mvnw spring-boot:run`) or PostgreSQL when run via `docker compose` — see [Run with Docker](#run-with-docker).

### Full Data Flow

```
REST API → DB save → RabbitMQ → Rule Engine → ETL Transformer → Idempotency
```

---

## Key Design Patterns

- **Event-driven architecture** — REST layer is fully decoupled from processing via RabbitMQ
- **Strategy Pattern** — pluggable rule engine; each source system has its own validation rules
- **Idempotent processing** — `deduplicationKey` unique constraint prevents double-processing
- **Dead Letter Queue** — failed messages are routed to DLQ after max retries; never lost
- **Reactive API** — WebFlux with `Mono`/`Flux`; blocking JPA calls offloaded to `boundedElastic` scheduler
- **Builder Pattern** — Lombok `@Builder` for clean, readable entity construction
- **Optimistic locking** — `@Version` on Transaction entity prevents silent data corruption
  when two consumers attempt to update the same row concurrently.
  The second writer gets `OptimisticLockException` and RabbitMQ retries automatically.
- **Defense in depth** — Duplicate detection at two layers: controller returns 409 at ingest,
  `IdempotentProcessor` catches any that reach the queue and marks them `DUPLICATE`.
---

## Run with Docker

The included `Dockerfile` and `docker-compose.yml` run the app together with
real PostgreSQL and RabbitMQ containers — no local Java, Maven, or RabbitMQ
install required.

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)

### Start everything

```bash
docker compose up --build
```

This builds the app image (multi-stage Maven build, then a slim JRE runtime
image) and starts three containers:

| Service | Image | Port(s) |
|---|---|---|
| `app` | built from `Dockerfile` | `8080` |
| `postgres` | `postgres:16-alpine` | `5432` |
| `rabbitmq` | `rabbitmq:3-management-alpine` | `5672` (AMQP), `15672` (management UI) |

The app waits for both `postgres` and `rabbitmq` to report healthy before
starting, and connects to **PostgreSQL** (not H2) via environment variables
that override `application.yml`.

- App: [http://localhost:8080](http://localhost:8080)
- RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672) — login: `guest` / `guest`

### Stop everything

```bash
docker compose down       # stop and remove containers
docker compose down -v    # also delete the Postgres/RabbitMQ data volumes
```

### AWS deployment

[AWS_deploy.md](AWS_deploy.md) walks through pushing this Docker image to
Amazon ECR and running it on ECS Fargate, using `ecs-task-definition.json`
as the task definition (RDS PostgreSQL and Amazon MQ replace the local
Postgres/RabbitMQ containers).

---

## Running Locally (without Docker)

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
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/api/v1/transactions" `
  -ContentType "application/json" `
  -Body '{"deduplicationKey":"timeout-test-001","sourceSystem":"BANK_API","transactionType":"PAYMENT","amount":500.00,"currency":"ILS","rawPayload":"{}"}'
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
├── rules/
│   ├── TransactionRule.java            # Strategy Pattern interface
│   ├── RuleResult.java                 # Immutable result record
│   ├── RuleEngine.java                 # Orchestrates all rules
│   ├── AmountValidationRule.java       # Rejects invalid amounts
│   ├── CurrencyValidationRule.java     # Allowlist + per-source restrictions
│   └── SourceSystemRule.java           # Enforces type/source combinations
├── etl/
│   └── ETLTransformer.java             # Enrichment, normalization, source mapping
├── processor/
│   ├── TransactionConsumer.java        # @RabbitListener pipeline orchestrator
│   ├── IdempotentProcessor.java        # Dedup + retry + DLQ routing
│   └── ProcessingResult.java           # Immutable outcome value object
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

The transaction is saved to the database and published to RabbitMQ before returning. The actual business processing (rule validation, ETL, idempotency check) happens in the consumer — asynchronously, after the HTTP response is already sent.

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
## Concurrency Model

The pipeline separates work across four distinct thread pools,
each optimized for its workload:

| Thread Pool | Threads | Purpose |
|---|---|---|
| Netty event loop | 2–4 | Non-blocking HTTP request handling |
| `boundedElastic` | up to 10 | Blocking JPA/DB operations |
| `pipelineExecutor` | 4–10 | Async RabbitMQ publishing |
| `ntContainer` | 1–5 | RabbitMQ message consumption |

### Why separate pools?
Each layer is isolated — a slow DB call never blocks the HTTP layer,
and a slow RabbitMQ publish never delays the HTTP response.

### Async publish flow
```
[boundedElastic]  save to DB → return 202 immediately
      │
      └──► [pipeline-1]  publishAsync() → RabbitMQ
                │
                └──► [ntContainer#0-1]  consume → Rules → ETL → persist
```
---

## Observability

All pipeline outcomes are instrumented with Micrometer counters and timers,
exposed via Spring Actuator at `/actuator/metrics`.

| Metric | Type | Description |
|---|---|---|
| `pipeline.transactions.processed` | Counter | Successfully processed transactions |
| `pipeline.transactions.failed` | Counter | Transactions that failed processing |
| `pipeline.transactions.duplicate` | Counter | Transactions rejected as duplicates |
| `pipeline.transactions.dlq` | Counter | Transactions routed to Dead Letter Queue |
| `pipeline.publish.timeout` | Counter | Async publish timeouts |
| `pipeline.processing.duration` | Timer | End-to-end processing time per transaction |

### Example queries

```powershell
# Processed count
GET /actuator/metrics/pipeline.transactions.processed

# Average processing time
GET /actuator/metrics/pipeline.processing.duration
```

### Production monitoring
Micrometer is vendor-neutral — the same counters feed into Prometheus,
DataDog, or AWS CloudWatch with zero code changes. Only configuration changes.

### Observed performance
Average processing time: ~26ms per transaction end-to-end.

---

## Testing

```powershell
./mvnw test
```

| Test class | What it covers |
|---|---|
| `AmountValidationRuleTest` | Boundary values, null handling, happy path |
| `RuleEngineTest` | Fail-fast behavior, mock interactions, rule ordering |
| `ETLTransformerTest` | Per-source amount conversion, currency normalization |

### Key testing decisions
- **Mockito** for `RuleEngineTest` — isolates the engine from real rule implementations.
  If a rule has a bug, only that rule's test fails, not the engine's.
- **No mocks** for `ETLTransformerTest` — pure business logic with no dependencies,
  tested directly with `new ETLTransformer()`.
- **`isEqualByComparingTo`** for all `BigDecimal` assertions —
  `equals()` on BigDecimal compares scale too (`100.1 ≠ 100.10`), which breaks
  financial comparisons. `compareTo` checks value only.
---
## Scope

This project implements the ingest-through-processing half of a financial
transaction pipeline:

- A reactive WebFlux REST API for single and batch transaction ingest, plus
  a mock data generator
- Persistence via Spring Data JPA, with optimistic locking (`@Version`) on
  the `Transaction` entity
- Event-driven processing over RabbitMQ — a durable queue, a Dead Letter
  Queue, and JSON message serialization
- A pluggable rule engine (Strategy Pattern) for per-source validation
- An ETL transformer for amount/currency normalization and source mapping
- Idempotent processing with a deduplication key, retry tracking, and DLQ
  routing, enforced at both the controller (409 on ingest) and consumer layers
- Async publishing on a bounded thread pool with a publish timeout
- Micrometer metrics exposed via Spring Actuator
- Docker Compose for local containerized runs (PostgreSQL + RabbitMQ) and
  an ECS Fargate task definition for AWS deployment
- Unit tests for the rule engine, ETL transformer, and controller layer;
  CI via GitHub Actions

**Not implemented:** a reconciliation/reporting stage (matching processed
transactions against an external source and generating CSV/JSON reports)
was planned but does not exist in the codebase.
