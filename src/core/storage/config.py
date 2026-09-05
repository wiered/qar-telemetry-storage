"""Runtime configuration and SSTable format identifiers for storage."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

SSTableFormat = Literal["v1_raw", "v2_timeseries"]
SUPPORTED_SSTABLE_FORMATS: frozenset[SSTableFormat] = frozenset(
    {"v1_raw", "v2_timeseries"}
)


@dataclass(frozen=True, slots=True)
class StorageRuntimeConfig:
    """Runtime configuration values for storage subsystem.

    Attributes:
        data_dir: Root directory where storage files are kept.
        flush_max_rows: Row threshold that triggers memtable flush.
        flush_max_points: Point threshold that triggers memtable flush.
        flush_max_bytes: Approximate memory threshold for flush.
        sstable_block_max_points: Maximum points per SST block.
        compaction_min_tables: Minimum SST count to start compaction.
        sstable_format: SST serialization format identifier.
        cleanup_temp_on_startup: Whether temporary files are cleaned on startup.
        quarantine_dir_name: Directory name for quarantined files.
    """

    data_dir: Path
    flush_max_rows: int = 10_000
    flush_max_points: int = 100_000
    flush_max_bytes: int = 8 * 1024 * 1024
    sstable_block_max_points: int = 256
    compaction_min_tables: int = 4
    sstable_format: SSTableFormat = "v2_timeseries"
    cleanup_temp_on_startup: bool = True
    quarantine_dir_name: str = "quarantine"

    @property
    def manifest_path(self) -> Path:
        """Return path to the primary manifest file."""
        return self.data_dir / "manifest.json"

    @property
    def manifest_backup_path(self) -> Path:
        """Return path to the backup manifest file."""
        return self.data_dir / "manifest.json.bak"

    @property
    def sst_dir(self) -> Path:
        """Return directory that stores SST files."""
        return self.data_dir / "sst"

    @property
    def quarantine_dir(self) -> Path:
        """Return directory where corrupted files are moved."""
        return self.data_dir / self.quarantine_dir_name

    def should_flush(
        self, rows_count: int, points_count: int, approx_bytes: int
    ) -> bool:
        """Check whether flush thresholds are exceeded.

        Args:
            rows_count: Current number of rows in memory.
            points_count: Current number of points in memory.
            approx_bytes: Approximate memory used by buffered data.

        Returns:
            True if any configured flush threshold is reached, else False.
        """
        if self.flush_max_rows > 0 and rows_count >= self.flush_max_rows:
            return True
        if self.flush_max_points > 0 and points_count >= self.flush_max_points:
            return True
        if self.flush_max_bytes > 0 and approx_bytes >= self.flush_max_bytes:
            return True
        return False

    @classmethod
    def from_settings(cls, settings: Any) -> "StorageRuntimeConfig":
        """Create runtime config from settings object.

        Args:
            settings: Application settings object with optional `storage` section.

        Returns:
            Parsed storage runtime configuration with default values applied.
        """
        storage_settings = getattr(settings, "storage", None)
        data_dir = Path(getattr(settings, "data_dir"))
        sstable_format = str(
            getattr(storage_settings, "sstable_format", "v2_timeseries")
        )
        if sstable_format not in SUPPORTED_SSTABLE_FORMATS:
            raise ValueError(f"unsupported SSTable format: {sstable_format}")
        sstable_format = cast(SSTableFormat, sstable_format)

        return cls(
            data_dir=data_dir,
            flush_max_rows=int(getattr(storage_settings, "flush_max_rows", 10_000)),
            flush_max_points=int(
                getattr(storage_settings, "flush_max_points", 100_000)
            ),
            flush_max_bytes=int(
                getattr(storage_settings, "flush_max_bytes", 8 * 1024 * 1024)
            ),
            sstable_block_max_points=int(
                getattr(storage_settings, "sstable_block_max_points", 256)
            ),
            compaction_min_tables=int(
                getattr(storage_settings, "compaction_min_tables", 4)
            ),
            sstable_format=sstable_format,
            cleanup_temp_on_startup=bool(
                getattr(storage_settings, "cleanup_temp_on_startup", True)
            ),
            quarantine_dir_name=str(
                getattr(storage_settings, "quarantine_dir_name", "quarantine")
            ),
        )
