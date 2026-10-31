# Module 7 — Hands-on Kafka Reliability Lab

## Goal

Connect theory to observable behavior in a Java/Spring Boot app and a local Kafka broker.

## Build path

1. Read the POM, Docker Compose, application configuration, topic definitions, and event record.
2. Trace POST /api/orders from validation to Kafka producer acknowledgement.
3. Follow the listener to failure injection and the database service.
4. Inspect the unique event ID constraint and explain replay-safe database insertion.
5. Read the error handler and DLT observer.
6. Publish a transient failure that recovers.
7. Publish a poison pill and confirm DLT arrival.
8. Publish five copies of one event ID and confirm only one DB insert.
9. Publish 1,000 events with 95% on one key and inspect partition distribution.
10. Run tests and record actual results.

## What this demonstrates

- Producer acknowledgement is separate from business completion.
- At-least-once redelivery can happen after a database commit but before offset commit.
- A unique event ID makes this demo's DB insert idempotent.
- Bounded retries make failures observable; DLT routing is not automatic remediation.
- Same-key traffic demonstrates partition concentration.
- A one-broker topology does not test broker failover or replicated durability.

## Exit criteria

Explain the crash window between DB commit and offset commit, why producer idempotence is different from consumer idempotency, why external payments need their own idempotency or reconciliation, and what an outbox changes.

Do not mark broker-level integration as passed unless you run it against a live broker and save the actual results.

See the [project guide](../../projects/kafka-reliability-lab/README.md).
