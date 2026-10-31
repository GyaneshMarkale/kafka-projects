# Module 2 — Ordering and Delivery Guarantees

## At-most-once

Commit progress before processing, or otherwise arrange that a failed record is not processed again. A crash after commit but before the side effect may lose the effect. This trades possible loss for avoiding repeated processing.

## At-least-once

Process the record, then commit progress. A crash after the side effect but before the offset commit can cause re-delivery. This is common because retries support recovery, but side effects must be idempotent or deduplicated.

## Exactly-once boundaries

Kafka transactions can coordinate consumed offsets and produced Kafka records in supported Kafka-to-Kafka processing flows. Consumers using read_committed participate in the intended visibility model. That does not atomically include arbitrary SQL transactions, email sends, or external payment provider calls.

## Two different forms of idempotence

- **Producer idempotence** suppresses duplicate records from supported producer retries.
- **Consumer/business idempotence** prevents repeating a side effect when the record is processed more than once. Examples: unique event ID constraint, inbox table, provider idempotency key, or compare-and-set status transition.

These solve different problems. Producer idempotence does not make a database update exactly once.

## Offset correctness

For sequential consumption, commit after successful processing. With concurrent asynchronous workers, only commit through the highest contiguous set of completed offsets per partition. Committing the highest finished offset can skip an earlier slow event after a crash.

## Failure timeline

1. Consumer reads offset 42.
2. A database insert commits.
3. The process crashes before offset 42 is committed.
4. Offset 42 is delivered again.
5. A unique event ID turns the repeated database insert into a no-op.

An external payment still requires provider-side idempotency or reconciliation.

## Interview checkpoints

1. Why can at-least-once lead to duplicates?
2. What does Kafka EOS cover?
3. What can be lost if offsets are committed too early?
4. Why are producer and consumer idempotence different?
5. Why is the highest completed offset unsafe with concurrent processing?

## Practice

Run the lab's duplicate-event endpoint and inspect the database count and logs. Explain the crash window between a database commit and offset commit.
