# ADR-002 — Apache Iceberg as the Table Format

**Status:** Accepted  
**Date:** 2026-06-29  
**Deciders:** Data Engineering team

---

## Context

The pipeline materialises two persistent datasets: bronze (raw ingestion) and silver (curated). These datasets must be:
- queryable via Athena without a separate ETL job or crawler
- writable from Python (no Spark cluster)
- partitionable for cost-efficient Athena scans
- capable of schema evolution as new source fields emerge
- registered in a central metadata catalog (Glue)

Options evaluated:
- Apache Iceberg
- Apache Hudi
- Delta Lake
- Plain Parquet files with Hive-style directories

## Decision

Use **Apache Iceberg format-version 2** for both bronze and silver tables, registered in **AWS Glue Data Catalog** as the Iceberg catalog. Written from Python via **PyIceberg**.

Silver is partitioned by `(order_year, order_month)` using IdentityTransform.  
Bronze is unpartitioned (append-only audit log — full scans are acceptable).

## Consequences

**Positive:**
- **Athena Engine v3 native support**: Athena reads Iceberg tables directly from Glue; no crawler, no `MSCK REPAIR TABLE`, no Glue job needed.
- **ACID writes**: concurrent writers are safe. A failed flush does not corrupt the table — the snapshot is either committed or not.
- **Schema evolution**: adding a column does not require rewriting existing files.
- **Time travel**: `SELECT * FROM sales_silver FOR SYSTEM_TIME AS OF …` for point-in-time debugging.
- **Partition pruning**: Athena skips irrelevant `order_year=*/order_month=*/` prefixes, reducing scan cost for date-range queries.
- **PyIceberg**: writable from Python without a Spark cluster. Table auto-created on first run, registered in Glue in the same operation.

**Negative / trade-offs:**
- PyIceberg requires `pyarrow` as the in-process serialiser, adding ~100MB to the Docker image.
- Iceberg metadata files (manifests, snapshots) accumulate over time — a periodic `expire_snapshots()` maintenance job should be scheduled.
- Small-file problem: writing 500-record micro-batches creates many small Parquet files. A compaction job (Spark or Athena OPTIMIZE) should run daily.

## Alternatives Considered

| Alternative | Why Rejected |
|---|---|
| Apache Hudi | No native Athena Engine v3 support for all features. Glue integration more complex. |
| Delta Lake | AWS-native support requires additional connectors. No PyDelta equivalent to PyIceberg. |
| Plain Parquet + Hive dirs | No ACID. Athena needs `MSCK REPAIR TABLE` after every write. No time travel. |
