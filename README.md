# Economics Data Ingestion Pipeline

End-to-end streaming ingestion pipeline for external economics datasets (IATA case study).  
Implements a **medallion architecture** (landing → bronze → silver) on AWS using Kafka (MSK), Apache Iceberg, and Terraform.

---

## Architecture

```
┌─────────────┐   HTTP/ZIP   ┌────────────┐   Kafka topic    ┌──────────────────┐
│  Source URL │ ──────────── │ Downloader │ ──────────────── │ consumer_bronze  │
│  (CSV file) │              │  Producer  │  sales-events    │                  │
└─────────────┘              └────────────┘                  └────────┬─────────┘
                                                                       │ BronzeRecord
                                                               ┌───────▼─────────┐
                                                               │  Bronze layer   │
                                                               │  (Iceberg/S3)   │
                                                               └───────┬─────────┘
                                                                       │ transform
                                                               ┌───────▼─────────┐
                                                               │  Silver layer   │
                                                               │  (Iceberg/S3)   │
                                                               └─────────────────┘
```

### Pipeline steps

| Step | Service | What it does |
|------|---------|--------------|
| 1 | `downloader` | Downloads the ZIP from the source URL, extracts the CSV to the landing zone |
| 2 | `producer` | Reads the CSV row-by-row, publishes each row as a JSON message to Kafka |
| 3 | `consumer_bronze` | Consumes from Kafka, validates each message, writes to the bronze Iceberg table |
| 4 | `transformer_silver` | Reads bronze, applies enrichment (margin %, lead time, year/month partitions), deduplicates, writes to silver |

### Delivery guarantees

- **At-least-once** delivery from Kafka (offsets committed after each flush batch of 500 records, not per message)
- **Idempotent local bronze** — one NDJSON file per flush batch, named `p{partition}_o{first_offset}.ndjson`; replaying the same offset range overwrites the same file
- **Cloud bronze** — Iceberg append-only; a replay adds a new Parquet file (by design — bronze is the raw audit layer, dedup happens at silver)
- **Silver deduplication** — `source_kafka_offset` is the idempotency key; replays are no-ops via in-memory seen-set pre-loaded from disk/Iceberg

---

## Repository layout

```
.
├── packages/
│   └── economics_pipeline/        # Shared library (installed editable)
│       ├── config/                #   PipelineSettings (pydantic-settings)
│       ├── dao/                   #   Bronze/Silver readers and writers
│       │   ├── base.py            #   Protocol definitions (interfaces)
│       │   ├── iceberg_dao_read_only.py
│       │   └── iceberg_dao_read_write.py
│       ├── exceptions/            #   Typed exception hierarchy
│       ├── kafka/                 #   Producer + consumer wrappers
│       ├── models/                #   SalesRecord, BronzeRecord, SilverRecord
│       ├── retry/                 #   Tenacity retry policies
│       └── transforms/            #   Bronze → silver pure transform
│
├── services/
│   ├── downloader/                # Step 1 — download + extract CSV
│   ├── producer/                  # Step 2 — CSV → Kafka
│   ├── consumer_bronze/           # Step 3 — Kafka → bronze layer
│   └── transformer_silver/        # Step 4 — bronze → silver layer
│
├── tests/
│   ├── unit/                      # No external dependencies (mocked Kafka)
│   ├── integration/               # Real Kafka via docker-compose
│   └── e2e/                       # Full 4-step pipeline, real Kafka + local FS
│
├── infra/                         # Terraform — AWS MSK, S3, Glue, Athena, IAM
├── docs/adr/                      # Architecture Decision Records
└── docker-compose.yml             # Local dev stack
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.11+ |
| Docker Desktop | 4.x (with WSL2 on Windows) |
| Terraform | 1.5+ (cloud deployment only) |

> **Windows / WSL2**: Docker Desktop requires at least 4 GB of memory allocated to WSL2.  
> Create `C:\Users\<you>\.wslconfig` with:
> ```ini
> [wsl2]
> memory=6GB
> processors=4
> ```
> Then run `wsl --shutdown` and restart Docker Desktop.

---

## Quick start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e packages/
pip install pytest pytest-mock
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — leave PIPELINE_S3_BUCKET empty for local dev
```

### 3. Start the local Kafka stack

```bash
docker compose up -d zookeeper kafka
```

### 4. Run the pipeline manually (step by step)

```bash
# Step 1+2 — download CSV and produce to Kafka
docker compose run --rm downloader
docker compose run --rm producer

# Step 3+4 — consume to bronze, then transform to silver
docker compose up consumer_bronze transformer_silver
```

Or run the full stack (includes the daily scheduler at 09:00 UTC):

```bash
docker compose up
```

---

## Running tests

### Unit tests (no Docker required)

```bash
pytest tests/unit/
```

### Integration tests (requires Docker)

Each test gets an isolated Kafka topic — Docker starts automatically via the fixture.

```bash
pytest tests/integration/ -m integration -v
```

### End-to-end tests (requires Docker)

Runs the complete pipeline (CSV → Kafka → bronze → silver) against a real local broker.  
Docker starts and stops automatically.

```bash
pytest tests/e2e/ -m e2e -v
```

### Run a single test

```bash
# Single unit test
pytest tests/unit/test_kafka_producer.py::TestSalesProducer::test_send_calls_kafka -v

# Single integration test
pytest tests/integration/test_producer_consumer.py::TestProducerConsumerRoundTrip::test_records_reach_consumer -v -s

# Single e2e test
pytest tests/e2e/test_full_pipeline.py::TestFullPipeline::test_producer_to_bronze -v -s
```

---

## Configuration

All settings are controlled via environment variables prefixed with `PIPELINE_`.  
See `.env.example` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_ENVIRONMENT` | `dev` | Environment name |
| `PIPELINE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `PIPELINE_KAFKA_TOPIC` | `sales-events` | Topic name |
| `PIPELINE_DATA_BASE_PATH` | `data` | Root path for local storage |
| `PIPELINE_S3_BUCKET` | _(empty)_ | Set to enable cloud mode (S3 + Glue) |
| `PIPELINE_MAX_ROWS` | _(none)_ | Cap rows per run — useful for dev/testing |
| `PIPELINE_SOURCE_URL` | _(eforexcel.com)_ | ZIP download URL |

**Cloud vs local**: setting `PIPELINE_S3_BUCKET` switches every DAO from local NDJSON files to real Iceberg tables in S3 + Glue. No code changes required.

---

## CI/CD

GitHub Actions runs on every push and pull request to `main`:

| Job | Trigger | What runs |
|-----|---------|-----------|
| `unit-tests` | Always | `pytest tests/unit/` — no external dependencies |
| `integration-tests` | After unit tests pass | `pytest tests/integration/ -m integration` with a Kafka service container |

See [.github/workflows/ci.yml](.github/workflows/ci.yml).

---

## Cloud deployment (AWS)

Infrastructure is managed with Terraform. Modules provision:

- **VPC** — private subnets, security groups
- **MSK** — Amazon Managed Streaming for Kafka (3 brokers)
- **S3** — data lake bucket (landing / bronze / silver / athena-results prefixes)
- **Glue Data Catalog** — Iceberg table metadata
- **Athena** — ad-hoc SQL queries on silver layer
- **IAM** — least-privilege roles per service
- **EventBridge + Step Functions** — daily scheduler at 09:00 UTC

```bash
cd infra/

# First time
terraform init -backend-config=environments/int.backend.hcl

# Plan and apply
terraform plan  -var-file=environments/int.tfvars
terraform apply -var-file=environments/int.tfvars
```

Available environments: `dev`, `int`, `uat`, `prod`.

---

## Architecture Decision Records

| ADR | Decision |
|-----|---------|
| [ADR-001](docs/adr/ADR-001-kafka-as-streaming-transport.md) | Kafka as streaming transport |
| [ADR-002](docs/adr/ADR-002-apache-iceberg-table-format.md) | Apache Iceberg as table format |
| [ADR-003](docs/adr/ADR-003-python-over-spark-for-transformation.md) | Python over Spark for transformation (+ migration path) |
| [ADR-004](docs/adr/ADR-004-at-least-once-with-deduplication.md) | At-least-once delivery with idempotent deduplication |
| [ADR-005](docs/adr/ADR-005-medallion-architecture.md) | Medallion architecture (landing → bronze → silver) |
