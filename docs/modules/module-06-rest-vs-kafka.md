# Module 6 — REST vs Event-Driven Architecture

## REST

Prefer synchronous REST when the caller needs an immediate authoritative answer: retrieve a profile, validate input, check current eligibility, or obtain a quote. It provides natural request-level success/failure reporting and is often simpler when the caller cannot proceed without the result.

Trade-offs include downstream latency, timeout management, and cascading failures in long call chains. Use timeouts, bounded retries, idempotency for repeatable writes, and tracing. A REST endpoint can accept work asynchronously with HTTP 202; the key distinction is whether the caller waits for the business outcome.

## Kafka

Prefer Kafka when one event should trigger several independent consumers, buffering is valuable, or the producer should not call every downstream service. Consumers can catch up from retained events after recovery, within capacity and retention limits.

Trade-offs include asynchronous completion, eventual consistency, duplicate processing, schema governance, lag monitoring, tracing, and operational complexity. A broker write acknowledgement does not mean consumer-side business processing has completed.

## Hybrid order example

1. REST request validates and creates an order with PENDING status.
2. The order row and an outbox record are written in one local database transaction.
3. A relay publishes OrderCreated to Kafka.
4. Inventory and payment consumers process independently and publish outcomes.
5. The order workflow updates status, and clients query that status with REST.

The outbox solves a database-plus-publish dual-write problem; consumers still need duplicate safety. A Saga or orchestrator may coordinate multi-step workflows and compensate when a later step fails.

## Events versus commands

An event describes a fact, such as OrderCreated or PaymentAuthorized. A command requests an action, such as ReserveInventory or AuthorizePayment. Events may have multiple subscribers; commands generally target a responsibility and can be rejected.

## Choose based on the business requirement

| Requirement | Default direction |
|---|---|
| Caller must get a current authoritative decision | REST |
| Independent consumers react to a domain fact | Kafka |
| Work can be deferred and needs durable buffering | Kafka |
| Immediate user validation | REST |
| High-volume activity feeds projections and analytics | Kafka |
| Multi-step distributed transaction-like workflow | Hybrid, explicit state, often Saga |

## Interview checkpoints

1. Why not replace every REST call with Kafka?
2. What does eventual consistency mean to a client?
3. How does outbox prevent a lost publish after DB commit?
4. Why doesn't outbox guarantee every external side effect exactly once?
5. How do correlation IDs help trace asynchronous workflows?

## Practice

Draw the hybrid order workflow, mark synchronous boundaries, identify duplicate windows, and explain how every side effect becomes safe to retry.
