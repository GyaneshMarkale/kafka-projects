# Module 3 — Consumer Reliability

## Rebalances and polling

A rebalance redistributes partition ownership when group membership or assignment conditions change. It can pause processing and move partitions to a new consumer.

- max.poll.interval.ms bounds time between poll calls before the consumer can be considered stuck.
- session.timeout.ms participates in deciding when a member has stopped heartbeating.
- max.poll.records caps records returned by one poll; it does not cap all underlying fetch buffers.

Do not perform unbounded slow work on the consumer thread. If work is delegated to workers, coordinate pause/resume, ordering, partition ownership, and offset commits.

## Lag

Lag is the gap between partition log end and consumer group progress. Rising lag is a symptom, not a diagnosis. Check producer arrival rate, processing rate, event age, assigned partitions, downstream latency, errors, retries, CPU, and GC.

## Retry patterns

- **Blocking retry:** retry before moving on. Simpler and can preserve partition order, but one poison record can stall later records on that partition.
- **Retry topic / non-blocking retry:** forwards failures for later retry and lets later records proceed, but those records can overtake the failed record.
- **Dead-letter topic:** holds records that exhaust retries for diagnosis and controlled replay. A DLT is not self-healing; ownership and alerting are required.

Bound attempts, use backoff, classify transient versus permanent errors, and avoid retry storms.

## Idempotency and schemas

Persist an event ID under a unique constraint with the consumer's database side effect. An outbox instead atomically saves the local database change and the intention to publish; the relay can still publish duplicates, so consumers remain idempotent.

Treat event schemas as contracts. Prefer safe additive evolution and test backward/forward compatibility policies. Changing field meaning is more dangerous than adding an optional field with a safe default.

## Interview checkpoints

1. What causes a rebalance?
2. Why can long processing violate max.poll.interval.ms?
3. When do retry topics change ordering?
4. What should an operational DLT runbook include?
5. How does outbox differ from inbox/deduplication?
6. What causes lag to keep increasing?

## Practice

Publish a transient failure configured to recover, then a poison record. Verify retry attempts and DLT arrival in application logs.
