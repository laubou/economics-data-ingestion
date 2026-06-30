# ADR-003 — Python over Spark for the Bronze → Silver Transformation

**Status:** Accepted  
**Date:** 2026-06-29  
**Deciders:** Data Engineering team

---

## Context

The `transformer_silver` service reads from the Iceberg bronze table and writes to the Iceberg silver table, applying type enforcement, deduplication, and field derivation. The transformation engine must:
- run within the existing Docker/ECS Fargate environment
- handle 2M records per daily batch reliably
- be maintainable by a small team without Spark expertise
- be replaceable if the volume grows significantly

Options evaluated:
- Python (pandas / pure iteration) on ECS Fargate
- AWS Glue PySpark job
- Amazon EMR Spark cluster
- Athena SQL CTAS/MERGE

## Decision

Use **Python (pure iteration via PyIceberg scan)** running in an ECS Fargate task. No Spark cluster.

The transformation is a pure function: `transform_to_silver(BronzeRecord) → SilverRecord`. It is tested independently of any execution engine. The DAO layer (`iceberg_dao_read_only.py`, `iceberg_dao_read_write.py`) abstracts the read/write so the engine can be swapped without touching service code.

## Consequences

**Positive:**
- **No cluster cost**: Fargate charges only for task runtime (~5 min/day for 2M rows).
- **Simple to test**: the transform function is pure Python, fully unit-testable without mocks.
- **Same Docker image as other services**: no separate Glue/EMR dependencies.
- **Fast iteration**: changing the transform logic requires only updating a Python file, not a Glue job or Spark JAR.

**Negative / trade-offs:**
- Single-threaded. For 2M rows this takes ~2-3 min on a 1-vCPU Fargate task. Acceptable for a daily batch.
- In-memory dedup set grows with the number of distinct silver records. At 50M+ unique records, the set may exhaust Fargate memory — at that point migrate to DynamoDB or Iceberg MERGE INTO.
- No SQL pushdown: all filtering happens in Python, not at the storage layer.

## When to Switch to Spark

Consider migrating `transformer_silver` to a **Glue PySpark job** when:
1. The daily record volume exceeds **50M rows** (Python single-thread takes >30 min).
2. Transformations require **SQL window functions** (e.g., running totals, lag/lead).
3. The team needs **multi-partition concurrent writes** for performance.

### Minimal Spark implementation (Glue)

The migration requires only:

1. Replace `services/transformer_silver/app.py` with a PySpark job:

```python
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.config(
    "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
).config(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog"
).config(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO"
).getOrCreate()

bronze = spark.table("glue_catalog.economics_pipeline.sales_bronze")

silver = (
    bronze
    .withColumn("order_year",    F.year("order_date"))
    .withColumn("order_month",   F.month("order_date"))
    .withColumn("lead_time_days", F.datediff("ship_date", "order_date"))
    .withColumn("margin_pct",
        F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2)
        .cast(DecimalType(6, 2)))
    .withColumn("unit_price",    F.col("unit_price").cast(DecimalType(18, 4)))
    # ... other decimal casts
)

# MERGE INTO handles dedup natively
spark.sql("""
    MERGE INTO glue_catalog.economics_pipeline.sales_silver t
    USING silver_staging s
    ON t.source_kafka_offset = s.source_kafka_offset
    WHEN NOT MATCHED THEN INSERT *
""")
```

2. Add a Terraform `aws_glue_job` resource pointing to the script on S3.
3. Replace the `transformer_silver` ECS task in Step Functions with a `states:::glue:startJobRun.sync` action.

**The DAO Protocol and all other services remain unchanged.**

## Alternatives Considered

| Alternative | Why Rejected |
|---|---|
| AWS Glue PySpark | Correct choice at scale. Rejected now because 2M rows/day doesn't justify the $0.44/DPU-hour cost and added complexity. |
| EMR cluster | Higher ops burden than Glue. No benefit for a batch that runs once per day. |
| Athena CTAS/MERGE | SQL-only transforms, no Python logic. `ROUND_HALF_UP` decimal semantics unavailable. MERGE INTO at 2M rows scans the full silver table each run. |
