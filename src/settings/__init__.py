"""Application configuration models and runtime settings instance.

This module defines nested Pydantic settings models loaded from environment
variables and an optional `.env` file.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parents[2]


class IngestSettings(BaseSettings):
    """Configuration for ingestion buffering and batching.

    Attributes:
        queue_max_frames: Maximum number of frames in the ingest queue.
        batch_max_points: Maximum number of points per emitted batch.
        batch_max_rows: Maximum number of rows per emitted batch.
        batch_max_ms: Maximum buffering time for a batch in milliseconds.
        overflow_policy: Queue overflow handling strategy.
        warn_every_dropped: Log warning after this many dropped items.
        idle_sleep_ms: Sleep interval when ingestion loop is idle.
    """

    queue_max_frames: int = 10_000
    batch_max_points: int = 50_000
    batch_max_rows: int = 1000
    batch_max_ms: int = 50
    overflow_policy: str = "drop_newest"
    warn_every_dropped: int = 1000
    idle_sleep_ms: int | float = 10


class StorageSettings(BaseSettings):
    """Configuration for storage flush, SSTable, and cleanup behavior.

    Attributes:
        flush_max_rows: Maximum rows before forcing a flush.
        flush_max_points: Maximum points before forcing a flush.
        flush_max_bytes: Maximum buffered bytes before forcing a flush.
        sstable_block_max_points: Maximum points per SSTable block.
        compaction_min_tables: Minimum table count to trigger compaction.
        sstable_format: Target SSTable format version identifier.
        cleanup_temp_on_startup: Remove temporary storage artifacts on startup.
        quarantine_dir_name: Directory name for corrupted artifacts.
    """

    flush_max_rows: int = 10_000
    flush_max_points: int = 100_000
    flush_max_bytes: int = 8 * 1024 * 1024
    sstable_block_max_points: int = 256
    compaction_min_tables: int = 4
    sstable_format: str = "v2_timeseries"
    cleanup_temp_on_startup: bool = True
    quarantine_dir_name: str = "quarantine"


class Settings(BaseSettings):
    """Top-level application settings loaded from `.env` and environment.

    Attributes:
        model_config: Pydantic settings dict (``.env`` path, nested ``__`` delimiter,
            ignore unknown env keys).
        data_dir: Root directory for application data.
        logging_level: Logging severity level.
        logging_format: Logging output format profile.
        host: Service bind host.
        port: Service bind port.
        hz: FDAU frequency in ticks per second.
        ingest: Nested ingestion configuration.
        storage: Nested storage configuration.
    """

    model_config = SettingsConfigDict(
        env_file=str(ROOT / ".env"),
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    data_dir: Path = ROOT / "data"

    logging_level: str = Field(
        default="INFO",
        description=(
            "Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). "
            "Can be set via LOGGING_LEVEL environment variable or .env file"
        ),
    )
    logging_format: str = Field(
        default="standard",
        description=(
            "Logging format ('standard' or 'detailed'). "
            "Can be set via LOGGING_FORMAT environment variable or .env file"
        ),
    )

    host: str = "0.0.0.0"
    port: int = 2201

    hz: int = Field(
        default=8,
        description=(
            "FDAU frequency in Hz (ticks per second). "
            "Can be set via HZ environment variable or .env file"
        ),
    )
    ingest: IngestSettings = IngestSettings()
    storage: StorageSettings = StorageSettings()


settings = Settings()
