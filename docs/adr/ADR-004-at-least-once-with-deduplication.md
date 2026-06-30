# ADR-004 — At-Least-Once Delivery with Application-Level Deduplication

**Status:** Accepted  
**Date:** 2026-06-29  
**Deciders:** Data Engineering team

---

## Context

The Kafka consumer must guarantee that no record is lost in transit from the bronze topic to the Iceberg bronze table. Two delivery semantics are available in Kafka:

- **At-least-once**: the consumer commits the offset only after a successful write. A failure between write and commit causes the record to be re-delivered. Duplicates are possible.
- **Exactly-once**: uses Kafka transactions and idempotent producers to guarantee each message is processed exactly once end-to-end. Significantly more complex to configure and operate.

The pipeline must also survive consumer restarts without re-processing records that have already been written.

## Decision

Use **at-least-once delivery** (`enable_auto_commit=False`, manual commit after successful flush) combined with **application-level deduplication** in the silver layer.

Dedup key: `source_kafka_offset` (format: `{topic}-{partition}-{offset}`). This key is immutable for a given Kafka message and is stored in `SilverRecord` as both a dedup key and an audit field.

On startup, `CloudSilverWriter` scans the existing silver table to load all known offsets into memory. Any record whose offset is already present is silently skipped.

## Consequences

**Positive:**
- **Simpler to implement and test**: no Kafka transactions, no idempotent producer configuration.
- **Restart-safe**: the pre-loaded offset set makes restarts after a failure completely idempotent — no duplicate records appear in silver even if the consumer replays messages.
- **Transparently auditable**: every silver record carries `source_kafka_offset` — you can always trace a silver row back to the exact Kafka message that produced it.
- **No bronze duplication**: the bronze filename `p{partition}_o{offset}.json` is also offset-based, making bronze writes idempotent by construction.

**Negative / trade-offs:**
- At scale (>50M unique offsets), the in-memory offset set may become a bottleneck. Migration path: replace the in-memory set with a DynamoDB conditional put or an Iceberg MERGE INTO.
- Duplicate records appear in bronze (by design — bronze is the raw audit layer). Dedup happens at silver, not before.
- If the silver table is truncated and replayed from bronze, the offset set starts empty and all records are re-written — this is the correct behaviour.

## Exactly-Once: When to Consider It

Exactly-once is appropriate when:
- Downstream systems cannot tolerate even transient duplicates (e.g., financial ledgers with immediate settlement).
- The consumer also produces to another Kafka topic (chained pipelines).

For an analytics pipeline where duplicates are resolved before serving, at-least-once + dedup is the standard industry pattern and simpler to operate.

## Alternatives Considered

| Alternative | Why Rejected |
|---|---|
| Exactly-once (Kafka transactions) | Requires `transactional.id`, `isolation.level=read_committed` on consumer, and idempotent producer. Adds ~40% latency. Not justified for a batch analytics pipeline. |
| Auto-commit | Commits offset before write confirmation. Risk of data loss on crash between commit and write. Rejected. |
| Dedup by `order_id` (business key) | More meaningful semantically but `order_id` is not guaranteed unique in the source data. `source_kafka_offset` is always unique within a topic-partition. |
