# Module 4 — Producer Performance and Durability

## Durability settings

- acks=0 trades broker confirmation for lower acknowledgement waiting.
- acks=1 waits for leader acknowledgement, which can leave a recent write vulnerable to some leader-failure windows.
- acks=all waits according to the current ISR and broker rules.
- min.insync.replicas sets the minimum ISR for accepting acks=all writes.

Replication durability cannot be demonstrated by a single broker with RF=1. Use a multi-broker cluster for failover experiments.

## Idempotence, retries, and timeouts

Producer idempotence helps suppress duplicates caused by supported producer retries. Preserve compatible defaults instead of mixing retry and in-flight settings arbitrarily. Retries improve resilience but can increase tail latency. Delivery timeout should reflect the maximum acceptable producer-side delivery duration.

## Batching and throughput

- batch.size controls the target per-partition batch size.
- linger.ms allows a short wait to collect more records per batch; throughput may improve at the cost of intentional latency.
- Compression trades CPU for fewer network/storage bytes.
- buffer.memory holds records while waiting for broker delivery.
- max.block.ms bounds certain waits for metadata or buffer availability; it is not an end-to-end business deadline.

Tune only after measuring send rate, request latency, batch size, compression ratio, retries/errors, and buffer pressure.

## Hot keys

A shared key concentrates its records in one partition to preserve key order. The hot partition can limit throughput while other partitions remain underused. Adding partitions does not divide a single key across all partitions without changing the ordering model.

## Interview checkpoints

1. How do acks and min.insync.replicas interact?
2. Why can linger.ms improve throughput?
3. Does producer idempotence protect database side effects?
4. Which metrics can show broker slowdown?
5. Why can one key dominate one partition?

## Practice

Publish 1,000 events with 95% sharing a key. Compare with uniformly distributed keys and inspect partition counts.
