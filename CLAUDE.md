# Financial Transaction Processing Pipeline

## Project Overview
Spring Boot WebFlux backend pipeline that ingests financial transactions,
validates, normalizes, deduplicates, and persists them via RabbitMQ.

## Tech Stack
- Java 21, Spring Boot 3, WebFlux, Spring AMQP
- PostgreSQL, JPA with optimistic locking
- RabbitMQ with DLQ
- Micrometer + Actuator
- JUnit 5, Mockito, GitHub Actions CI

## Working Style
- Explain every file and every non-obvious line — I'm learning Docker and AWS
- Hebrew explanations are fine
- Don't skip steps, even if they seem obvious
- After each change, tell me what to run and what to expect to see

## Current Goal
Add Docker support: Dockerfile + docker-compose with Postgres and RabbitMQ