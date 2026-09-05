"""Tests for StorageCore queries, flush, recovery, and configuration."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.storage import StorageCore, StorageRuntimeConfig
from core.storage import core as storage_core_module


def make_config(tmp_path: Path, **overrides: Any) -> StorageRuntimeConfig:
    """Build a StorageRuntimeConfig rooted under ``tmp_path`` for tests.

    Args:
        tmp_path: Temporary directory; ``data_dir`` becomes ``tmp_path / "storage"``.
        **overrides: Keyword arguments overriding default StorageRuntimeConfig fields.

    Returns:
        Config instance passed to StorageCore in tests.
    """
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 2),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 2),
        compaction_min_tables=overrides.get("compaction_min_tables", 4),
        sstable_format=overrides.get("sstable_format", "v2_timeseries"),
        cleanup_temp_on_startup=overrides.get("cleanup_temp_on_startup", True),
        quarantine_dir_name=overrides.get("quarantine_dir_name", "quarantine"),
    )


def test_storage_core_append_flush_and_query(tmp_path):
    """Append, flush to SSTable, and query back all points."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))

    storage.append_rows([(100, [(1, 1.0), (2, 2.0)]), (110, [(3, 3.0)])])
    storage.flush()

    points = storage.query_range(100, 120, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 100, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 110, "parameter_id": 3, "value": 3.0},
    ]


def test_storage_core_queries_across_memtable_and_sstable(tmp_path):
    """query_range merges rows still in the memtable with flushed SSTables."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=2))

    storage.append_rows([(100, [(2, 2.0)]), (90, [(1, 1.0)])])
    storage.append_rows([(105, [(3, 3.0)])])

    points = storage.query_range(0, 200, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 90, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 100, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 105, "parameter_id": 3, "value": 3.0},
    ]


def test_storage_core_query_range_uses_newest_wins_across_memtable_and_sstables(
    tmp_path,
):
    """Duplicate keys resolve to the newest row; filters apply correctly."""
    storage = StorageCore(
        config=make_config(tmp_path, flush_max_rows=100, compaction_min_tables=10)
    )

    storage.append_rows([(100, [(1, 1.0), (2, 2.0)]), (110, [(1, 11.0)])])
    storage.flush()
    storage.append_rows([(100, [(1, 10.0)]), (105, [(3, 3.0)])])
    storage.flush()
    storage.append_rows([(100, [(1, 100.0)]), (90, [(4, 4.0)]), (105, [(3, 30.0)])])

    points = storage.query_range(0, 200, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 90, "parameter_id": 4, "value": 4.0},
        {"timestamp_ns": 100, "parameter_id": 1, "value": 100.0},
        {"timestamp_ns": 100, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 105, "parameter_id": 3, "value": 30.0},
        {"timestamp_ns": 110, "parameter_id": 1, "value": 11.0},
    ]

    filtered = storage.query_range(95, 106, {1, 3})
    assert [point.to_dict() for point in filtered] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 100.0},
        {"timestamp_ns": 105, "parameter_id": 3, "value": 30.0},
    ]


def test_storage_core_recovers_after_close(tmp_path):
    """Reopening storage reloads flushed data from disk."""
    config = make_config(tmp_path, flush_max_rows=100)
    storage = StorageCore(config=config)
    storage.append_rows([(100, [(1, 1.0)]), (120, [(3, 3.0)])])
    storage.close()

    recovered = StorageCore(config=config)
    points = recovered.query_range(100, 121, {1, 3})

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 120, "parameter_id": 3, "value": 3.0},
    ]
    recovered.close()


def test_storage_core_filters_and_sorts_results(tmp_path):
    """Parameter filter keeps only requested ids in timestamp order."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))
    storage.append_rows(
        [
            (120, [(4, 4.0), (1, 1.0)]),
            (100, [(2, 2.0)]),
            (110, [(3, 3.0)]),
        ]
    )

    points = storage.query_range(105, 130, {1, 4})

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 120, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 120, "parameter_id": 4, "value": 4.0},
    ]


def test_storage_core_skips_non_overlapping_sstables(tmp_path, monkeypatch):
    """Time-range pruning avoids opening SSTables that cannot overlap."""
    storage = StorageCore(
        config=make_config(tmp_path, flush_max_rows=1, compaction_min_tables=10)
    )
    storage.append_rows([(10, [(1, 1.0)])])
    storage.append_rows([(20, [(2, 2.0)])])
    storage.append_rows([(30, [(3, 3.0)])])

    real_reader = storage_core_module.SSTableReader
    opened_files: list[str] = []

    class TrackingReader(real_reader):
        def __init__(self, file_path):
            opened_files.append(Path(file_path).name)
            super().__init__(file_path)

    monkeypatch.setattr(storage_core_module, "SSTableReader", TrackingReader)

    points = storage.query_range(15, 25, {2})

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 20, "parameter_id": 2, "value": 2.0}
    ]
    assert opened_files == ["sst_00000000000000000002.sst"]


def test_storage_core_rejects_operations_after_close(tmp_path):
    """Append, query, and compact raise after close."""
    storage = StorageCore(config=make_config(tmp_path))
    storage.close()

    with pytest.raises(RuntimeError, match="closed"):
        storage.append_rows([(1, [(1, 1.0)])])

    with pytest.raises(RuntimeError, match="closed"):
        storage.query_range(0, 1, None)

    with pytest.raises(RuntimeError, match="closed"):
        storage.compact()


def test_storage_runtime_config_from_settings_uses_direct_data_dir():
    """from_settings maps top-level ``data_dir`` and nested storage options."""
    settings = SimpleNamespace(
        data_dir=Path("C:/tmp/qar-data"),
        storage=SimpleNamespace(
            flush_max_rows=11,
            flush_max_points=22,
            flush_max_bytes=33,
            sstable_block_max_points=44,
            compaction_min_tables=5,
            sstable_format="v1_raw",
            cleanup_temp_on_startup=False,
            quarantine_dir_name="bad-files",
        ),
    )

    config = StorageRuntimeConfig.from_settings(settings)

    assert config.data_dir == Path("C:/tmp/qar-data")
    assert config.flush_max_rows == 11
    assert config.flush_max_points == 22
    assert config.flush_max_bytes == 33
    assert config.sstable_block_max_points == 44
    assert config.compaction_min_tables == 5
    assert config.sstable_format == "v1_raw"
    assert config.cleanup_temp_on_startup is False
    assert config.quarantine_dir_name == "bad-files"


def test_storage_core_reads_mixed_v1_and_v2_tables(tmp_path):
    """Queries combine SSTables written in v1 and v2 formats."""
    baseline_config = make_config(
        tmp_path,
        flush_max_rows=100,
        compaction_min_tables=10,
        sstable_format="v1_raw",
    )
    storage = StorageCore(config=baseline_config)
    storage.append_rows([(100, [(1, 1.0)])])
    storage.flush()
    storage.close()

    optimized_config = make_config(
        tmp_path,
        flush_max_rows=100,
        compaction_min_tables=10,
        sstable_format="v2_timeseries",
    )
    storage = StorageCore(config=optimized_config)
    storage.append_rows([(110, [(2, 2.0)])])
    storage.flush()
    points = storage.query_range(0, 200, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 110, "parameter_id": 2, "value": 2.0},
    ]
    storage.close()


def test_storage_core_stats_snapshot_reports_read_path_counters(tmp_path):
    """stats_snapshot reflects files/blocks considered during query_range."""
    storage = StorageCore(
        config=make_config(tmp_path, flush_max_rows=1, compaction_min_tables=10)
    )
    storage.append_rows([(10, [(1, 1.0)])])
    storage.append_rows([(20, [(2, 2.0)])])

    before = storage.stats_snapshot()
    points = storage.query_range(0, 30, {2})
    after = storage.stats_snapshot()

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 20, "parameter_id": 2, "value": 2.0}
    ]
    assert after.files_considered > before.files_considered
    assert after.files_pruned > before.files_pruned
    assert after.blocks_considered >= after.blocks_scanned
    assert after.points_returned > before.points_returned
