from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    """
    Single source of truth for all pipeline configuration.

    Values are resolved in priority order:
      1. Environment variables (prefixed PIPELINE_)
      2. .env file (path controlled by ENV_FILE, defaults to .env)
      3. Defaults below

    Set PIPELINE_ENVIRONMENT to switch between dev/int/uat/prod profiles.
    """

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Allow extra fields so environment-specific .env files don't break
        extra="ignore",
    )

    environment: str = "dev"

    # --------------- Kafka ---------------
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "sales-events"
    kafka_group_id: str = "bronze-consumer-group"
    kafka_num_partitions: int = 3
    # 1 for local dev (single broker), >=3 in cloud
    kafka_replication_factor: int = 1
    # -1 = wait forever (production). Set to e.g. 10000 in dev to exit cleanly after idle.
    kafka_consumer_timeout_ms: int = -1

    # --------------- Storage paths ---------------
    # In dev these are local directories; in cloud they become S3 URIs via properties below
    data_base_path: str = "data"
    s3_bucket: str = ""

    # --------------- AWS ---------------
    aws_region: str = "eu-west-1"
    glue_database: str = "economics_pipeline"
    athena_output_prefix: str = "athena-results/"

    # --------------- Source ---------------
    source_url: str = (
        "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"
    )
    source_filename: str = "2m Sales Records.csv"

    # --------------- Ingestion schedule ---------------
    # Cron expression used by the scheduler service.
    # Default: daily at 09:00 UTC (matching the batch trigger requirement).
    # AWS EventBridge uses the same cron syntax.
    ingestion_cron: str = "0 9 * * *"

    # --------------- Dev safety guard ---------------
    # Set to a positive integer to cap rows during development; None = unlimited
    max_rows: int | None = None

    # ------------------------------------------------------------------ #
    # Computed path properties — services use these, never raw strings    #
    # ------------------------------------------------------------------ #

    @property
    def is_cloud(self) -> bool:
        return bool(self.s3_bucket)

    @property
    def state_path(self) -> str:
        """Path to the ingestion state file (local JSON or S3 key)."""
        return (
            f"s3://{self.s3_bucket}/state/ingestion_state.json"
            if self.is_cloud
            else f"{self.data_base_path}/state/ingestion_state.json"
        )

    @property
    def landing_path(self) -> str:
        return f"s3://{self.s3_bucket}/landing" if self.is_cloud else f"{self.data_base_path}/landing"

    @property
    def archive_path(self) -> str:
        return f"s3://{self.s3_bucket}/archive" if self.is_cloud else f"{self.data_base_path}/archive"

    @property
    def bronze_path(self) -> str:
        return f"s3://{self.s3_bucket}/bronze" if self.is_cloud else f"{self.data_base_path}/bronze"

    @property
    def silver_path(self) -> str:
        return f"s3://{self.s3_bucket}/silver" if self.is_cloud else f"{self.data_base_path}/silver"


@lru_cache(maxsize=1)
def get_settings() -> PipelineSettings:
    """Return a cached settings singleton — safe to call from anywhere."""
    return PipelineSettings()
