# ADR-001 — Kafka / Amazon MSK as the Streaming Transport

**Status:** Accepted  
**Date:** 2026-06-29  
**Deciders:** Data Engineering team

---

## Context

The pipeline must move records from a flat CSV file in S3 landing/ to an Iceberg bronze table in near-real-time. Multiple downstream consumers may emerge (bronze writer today, analytics tomorrow, ML feature store later). The ingestion component (producer) and the storage component (consumer) must be able to evolve independently.

Options evaluated:
- Amazon Kinesis Data Streams
- Amazon MSK (managed Kafka)
- S3 event notifications → Lambda
- Direct file-to-Iceberg write (no message bus)

## Decision

Use **Amazon MSK (Kafka)** as the streaming transport between the producer and all downstream consumers.

Messages are keyed by `order_id` to guarantee that all events for the same order land on the same partition and are processed in order by any stateful consumer.

## Consequences

**Positive:**
- **Replay**: any consumer can re-read from offset 0 if it fails or a new consumer is added. This is the primary driver — S3 events and Lambda are fire-and-forget.
- **Decoupling**: the producer and consumer have no shared state. The producer can run on a different schedule than the consumers.
- **Fan-out**: a second consumer can be added (e.g., real-time anomaly detection) without touching the producer.
- **Back-pressure**: consumers control their own consumption rate via manual offset commit; the producer is never blocked by a slow consumer.
- **At-least-once + dedup**: easier to reason about than exactly-once semantics; duplicates are handled by the silver layer.

**Negative / trade-offs:**
- Adds operational complexity (broker management, topic creation, ZooKeeper/KRaft).
- MSK costs ~3× more than Kinesis per shard-hour on small workloads.
- For 2M records/day, a single Kafka topic with 3 partitions is over-provisioned. Scale-down to 1 broker in dev saves cost.

## Alternatives Considered

| Alternative | Why Rejected |
|---|---|
| Kinesis Data Streams | No replay beyond 24h (7-day max). Harder to run locally. |
| S3 events → Lambda | Fire-and-forget. No replay. Hard to test. Lambda cold starts add latency. |
| Direct CSV → Iceberg | No decoupling. If the writer fails mid-file, there is no checkpoint — full re-read required. |
