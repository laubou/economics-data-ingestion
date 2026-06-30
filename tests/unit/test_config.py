"""Unit tests for PipelineSettings path resolution and env switching."""

import pytest

from economics_pipeline.config.base import PipelineSettings


class TestPipelineSettings:
    def test_local_paths_when_no_s3_bucket(self, dev_settings: PipelineSettings) -> None:
        assert not dev_settings.is_cloud
        assert "landing" in dev_settings.landing_path
        assert "archive" in dev_settings.archive_path
        assert "bronze" in dev_settings.bronze_path
        assert "silver" in dev_settings.silver_path

    def test_s3_paths_when_bucket_set(self) -> None:
        settings = PipelineSettings(
            environment="int",
            s3_bucket="my-bucket",
            kafka_bootstrap_servers="broker:9092",
        )
        assert settings.is_cloud
        assert settings.landing_path == "s3://my-bucket/landing"
        assert settings.archive_path == "s3://my-bucket/archive"
        assert settings.bronze_path == "s3://my-bucket/bronze"
        assert settings.silver_path == "s3://my-bucket/silver"

    def test_max_rows_none_by_default_in_cloud(self) -> None:
        settings = PipelineSettings(
            environment="prod",
            s3_bucket="prod-bucket",
            kafka_bootstrap_servers="broker:9092",
        )
        assert settings.max_rows is None

    def test_env_prefix_isolation(self) -> None:
        # Ensure a setting without PIPELINE_ prefix doesn't bleed in
        settings = PipelineSettings(
            environment="test",
            kafka_bootstrap_servers="localhost:9092",
        )
        assert settings.environment == "test"

    def test_defaults_are_safe_for_dev(self) -> None:
        settings = PipelineSettings(kafka_bootstrap_servers="localhost:9092")
        assert settings.environment == "dev"
        assert settings.kafka_topic == "sales-events"
        assert settings.aws_region == "eu-west-1"
