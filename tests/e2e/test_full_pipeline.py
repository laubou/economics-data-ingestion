"""
End-to-end tests: full pipeline from CSV → Kafka → bronze → silver.

Requirements: docker-compose up (Kafka on localhost:9092).
Run: pytest tests/e2e/ -m e2e

Tests run in order within the class — each builds on the state left by
the previous one (shared session-scoped settings + tmp_path).
"""

import os
import threading

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.dao.iceberg_dao_read_only import LocalBronzeReader
from economics_pipeline.dao.iceberg_dao_read_write import (
    LocalBronzeWriter,
    LocalSilverWriter,
)
from economics_pipeline.kafka.consumer import BronzeConsumer
from economics_pipeline.kafka.producer import SalesProducer
from economics_pipeline.models.sales import SalesRecord
from economics_pipeline.transforms.silver import transform_to_silver


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _produce_from_csv(settings: PipelineSettings, csv_path: str) -> int:
    import csv as _csv

    sent = 0
    with SalesProducer(settings) as producer:
        with open(csv_path, encoding="utf-8") as f:
            for i, row in enumerate(_csv.DictReader(f)):
                if settings.max_rows and i >= settings.max_rows:
                    break
                producer.send(SalesRecord.from_csv_row(row))
                sent += 1
    # __exit__ calls flush() + close() — all messages are in Kafka before we return
    return sent


def _consume_to_bronze(
    settings: PipelineSettings, writer: LocalBronzeWriter, count: int
) -> None:
    received = 0
    with BronzeConsumer(settings) as consumer:
        for record in consumer.consume():
            writer.append(record)
            received += 1
            if received >= count:
                break
    writer.flush()


# ------------------------------------------------------------------ #
# E2E tests — run in definition order, each builds on previous state  #
# ------------------------------------------------------------------ #


@pytest.mark.e2e
class TestFullPipeline:

    def test_producer_to_bronze(
        self, e2e_settings: PipelineSettings, e2e_csv: str
    ) -> None:
        """CSV → Kafka → bronze: verify all rows land in bronze layer."""
        writer = LocalBronzeWriter(e2e_settings)
        n = e2e_settings.max_rows or 20

        # Produce first — all messages committed to Kafka before consumer starts.
        sent = _produce_from_csv(e2e_settings, e2e_csv)
        assert sent > 0, "CSV produced 0 records — check e2e_csv fixture"

        # Consume after — auto_offset_reset="earliest" replays from offset 0,
        # no race condition possible.
        thread = threading.Thread(
            target=_consume_to_bronze,
            args=(e2e_settings, writer, n),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=60)

        assert not thread.is_alive(), (
            "Consumer thread timed out after 60s — Kafka may be unreachable "
            "or messages were not delivered. Check 'docker logs kafka'."
        )

        bronze_files = os.listdir(e2e_settings.bronze_path)
        assert len(bronze_files) == sent, (
            f"Expected {sent} bronze files, got {len(bronze_files)}"
        )

    def test_bronze_to_silver(self, e2e_settings: PipelineSettings) -> None:
        """Bronze → silver: verify transform produces correct silver records."""
        reader = LocalBronzeReader(e2e_settings)
        writer = LocalSilverWriter(e2e_settings)

        bronze_records = list(reader.read_all())
        assert bronze_records, (
            "Bronze is empty — test_producer_to_bronze must pass first"
        )

        for bronze in bronze_records:
            silver = transform_to_silver(bronze)
            writer.merge(silver)
        writer.flush()

        all_silver = [
            f for _, _, fs in os.walk(e2e_settings.silver_path) for f in fs
        ]
        assert len(all_silver) == len(bronze_records), (
            f"Expected {len(bronze_records)} silver files, got {len(all_silver)}"
        )

    def test_silver_dedup_on_replay(self, e2e_settings: PipelineSettings) -> None:
        """
        Replaying the same bronze records must not create duplicate silver records.

        LocalSilverWriter.__init__ calls _load_existing_keys() to pre-populate
        the dedup set from disk — so a fresh writer after crash/restart still
        skips already-written offsets.
        """
        reader = LocalBronzeReader(e2e_settings)
        bronze_records = list(reader.read_all())
        assert bronze_records, (
            "Bronze is empty — test_producer_to_bronze must pass first"
        )

        before = sum(len(fs) for _, _, fs in os.walk(e2e_settings.silver_path))
        assert before > 0, (
            "Silver is empty — test_bronze_to_silver must pass first"
        )

        # Fresh writer — simulates restart. _load_existing_keys() re-loads
        # all source_kafka_offset values so replay is a no-op.
        writer = LocalSilverWriter(e2e_settings)
        for bronze in bronze_records:
            writer.merge(transform_to_silver(bronze))
        writer.flush()

        after = sum(len(fs) for _, _, fs in os.walk(e2e_settings.silver_path))
        assert after == before, (
            f"Replay created duplicates: silver file count went {before} → {after}"
        )

    def test_silver_has_correct_derived_fields(
        self, e2e_settings: PipelineSettings
    ) -> None:
        """Silver records must have all computed fields populated correctly."""
        import json

        checked = 0
        for root, _, files in os.walk(e2e_settings.silver_path):
            for filename in files:
                if not filename.endswith(".json"):
                    continue
                with open(os.path.join(root, filename)) as f:
                    record = json.load(f)
                assert record["order_year"] == 2020
                assert record["order_month"] == 1
                assert record["lead_time_days"] == 7
                assert float(record["margin_pct"]) > 0
                assert record["source_kafka_offset"] != ""
                checked += 1

        assert checked > 0, (
            "No silver files found — test_bronze_to_silver must pass first"
        )

    def test_no_data_loss(self, e2e_settings: PipelineSettings) -> None:
        """Every bronze record must have a corresponding silver record."""
        import json

        reader = LocalBronzeReader(e2e_settings)
        bronze_keys = {r.record_key for r in reader.read_all()}
        assert bronze_keys, "Bronze is empty — test_producer_to_bronze must pass first"

        silver_keys: set[str] = set()
        for root, _, files in os.walk(e2e_settings.silver_path):
            for filename in files:
                if filename.endswith(".json"):
                    with open(os.path.join(root, filename)) as f:
                        silver_keys.add(json.load(f)["source_kafka_offset"])

        missing = bronze_keys - silver_keys
        assert not missing, (
            f"{len(missing)} bronze record(s) have no silver counterpart: "
            f"{sorted(missing)[:5]}{'...' if len(missing) > 5 else ''}"
        )
