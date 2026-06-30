# ADR-005 — Medallion Architecture (Landing → Bronze → Silver)

**Status:** Accepted  
**Date:** 2026-06-29  
**Deciders:** Data Engineering team

---

## Context

The pipeline must store data at multiple stages of processing, from the raw source file through to a clean, queryable dataset. A storage architecture must be chosen that balances auditability, recovery capability, query performance, and operational simplicity.

Options evaluated:
- Single-layer: transform in-flight and write only the final dataset
- Two-layer: raw + curated
- Three-layer: landing + bronze + silver (medallion)
- Four-layer: landing + bronze + silver + gold (aggregates)

## Decision

Use a **three-layer medallion architecture**: landing, bronze, silver.

| Layer | Location | Format | Purpose |
|---|---|---|---|
| **landing/** | S3 | CSV (extracted from zip) | Raw source file, immutable. Input to the producer. |
| **archive/** | S3 | ZIP | Original compressed file, immutable. Audit trail. |
| **bronze/** | S3 Iceberg | Parquet | Raw records + Kafka metadata. Never modified after write. |
| **silver/** | S3 Iceberg | Parquet | Typed, deduped, partitioned. The queryable truth. |

A gold layer (pre-aggregated summaries) is not created — Athena can compute aggregations on-the-fly from silver at acceptable cost.

## Consequences

**Positive:**
- **Reprocessability**: if the silver transformation logic changes (e.g., new margin formula), bronze is replayed from scratch with zero data loss. No need to re-download the source file.
- **Auditability**: every silver record carries `source_kafka_offset`, `bronze_ingested_at`, and `silver_transformed_at` — full chain of custody.
- **Failure isolation**: a bug in the silver transform cannot corrupt bronze. The pipeline recovers by truncating silver and replaying.
- **Independent scaling**: the producer, bronze consumer, and silver transformer are independent services. Each scales to its own workload.
- **Progressive quality**: analysts can query bronze (raw) or silver (curated) depending on their tolerance for data quality.

**Negative / trade-offs:**
- Storage cost is ~3× a single-layer approach (landing + bronze + silver).
- Additional operational complexity: two Iceberg tables instead of one, two consumer services instead of one.
- Without a compaction job, small Parquet files accumulate in bronze (one file per 500-record flush). A daily Iceberg `OPTIMIZE` should be added.

## Why No Gold Layer

A gold layer (pre-computed aggregates) is deferred because:
1. Athena Engine v3 with partition pruning on `(order_year, order_month)` computes `GROUP BY country` on 2M rows in under 5 seconds.
2. Adding a gold layer requires another service, another table, and another scheduled job — unjustified for the current query volume.
3. If a specific aggregation becomes a hot path (e.g., daily revenue dashboard), it can be added as a materialised view or a named Athena named query with a cached result.

## Alternatives Considered

| Alternative | Why Rejected |
|---|---|
| Single layer (write only silver) | Cannot replay silver if transform logic changes — must re-download the source file. No audit trail. |
| Two layers (landing + silver) | Same as single layer but preserves the CSV. Still no independent replay from a typed, Kafka-aware checkpoint. |
| Four layers (+ gold) | Pre-aggregations premature at 2M rows/day. Athena handles it without pre-computation. |
