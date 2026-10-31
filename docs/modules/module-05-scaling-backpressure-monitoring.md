# Module 5 — Scaling, Backpressure, and Monitoring

## Scaling and throughput

The useful parallelism of a consumer group is constrained by partition assignments and by downstream capacity. More consumers cannot split a single partition's assigned work across several group members at the same time. Database bottlenecks, API rate limits, CPU, serialization, and hot-key skew can dominate.

Let arrival rate be λ and sustained processing rate be μ. If λ stays above μ, backlog grows at approximately the difference over the measurement window. During an outage, retention and storage capacity must accommodate the backlog, and consumers need enough recovery capacity to catch up.

## Backpressure

Bound work queues and thread pools. Apply timeouts and controlled retry policies. Pause consumption when a downstream dependency cannot accept work, then resume under container-aware coordination. max.poll.records limits records returned per poll but does not by itself cap all fetch buffers.

## Offset safety with async work

If offsets 10, 11, and 12 are processed by separate workers and offset 12 finishes first, committing offset 13 can be unsafe. A crash could restart at 13 while 10 or 11 is incomplete. Track completion per partition and only commit through the highest contiguous completed offset, or process sequentially per partition.

## Failure playbook

- Consumer crash: expect reassignment and possible redelivery; make side effects idempotent.
- Database outage: bound concurrency and retry pressure; expose dependency health.
- Poison message: bounded attempts, error classification, DLT, alerting, owner, and controlled replay.
- Rebalance loop: inspect poll timing, GC pauses, long calls, heartbeats, and consumer errors.
- Backlog spike: compare arrival/processing rates, oldest event age, assigned partitions, downstream latency, and retry volume.
- Hot partition: inspect keys and per-partition lag; revisit ordering requirements before changing the key strategy.

## Monitoring

Observe lag trend and event age, record consume rate, max lag, time between polls, last poll age, poll idle ratio, assigned partition counts, fetch/commit latency/errors, retries, DLT volume, downstream latency, queue depth, CPU, memory, and GC. Metric names vary by client/exporter version; verify the metric reference for the deployed version.

## Interview checkpoints

1. Why might more consumers not reduce lag?
2. What does λ > μ imply?
3. Why is committing the highest completed offset unsafe?
4. How does pause/resume provide backpressure?
5. Which signals distinguish hot-key skew from a slow database?

## Practice

Use the skew endpoint, inspect per-partition counts and lag, and propose an alert plus diagnostic path.
