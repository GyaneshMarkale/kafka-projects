# Module 1 — Kafka Fundamentals and Architecture

## Goal

Explain how Kafka stores records, replicates partitions, and divides work across consumer groups.

## Core terms

- **Producer:** publishes a record with a key, value, optional headers, and timestamp.
- **Topic:** named stream of records.
- **Partition:** ordered append-only log inside a topic. Partitions enable distribution and parallelism.
- **Offset:** position of a record within a partition; it is not a topic-global sequence.
- **Broker:** Kafka server that stores and serves partition replicas.
- **Leader replica:** normally handles reads and writes for a partition.
- **Follower replica:** fetches from the leader to keep a copy.
- **ISR:** in-sync replicas eligible under the cluster's replication rules.
- **Consumer group:** consumers with the same group ID divide partition assignments. One partition is assigned to at most one active group member at a time.

## Ordering and parallelism

A key such as orderId commonly routes related records to one partition. Kafka preserves append order within a partition, but not a total order across multiple partitions. With six partitions, a group cannot usefully achieve more than six-way partition-level parallelism at a time. More consumers can remain idle. Another group can independently consume the same topic.

## Durability

- acks=0: the producer does not wait for broker acknowledgement.
- acks=1: acknowledgement from the leader.
- acks=all: acknowledgement according to the in-sync replica and broker configuration.
- min.insync.replicas: minimum ISR needed for writes using acks=all.

RF=1 on one broker is not highly available. Replication and ISR failure scenarios require a multi-broker setup.

## Interview checkpoints

1. What is the difference between a partition and an offset?
2. Why does consumer parallelism stop scaling after the partition count?
3. Why is ordering scoped to one partition?
4. How do RF, ISR, acks=all, and min.insync.replicas interact?
5. How is a second consumer group different from a second consumer in one group?

## Practice

Publish repeated records with the same key and records with many distinct keys. Predict partition assignment and compare the result with broker tooling.

## Further reading

- https://kafka.apache.org/documentation/
- https://kafka.apache.org/43/operations/monitoring/
