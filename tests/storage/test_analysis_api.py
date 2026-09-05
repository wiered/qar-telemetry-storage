"""Tests for storage aggregate/query analysis helpers."""

from pathlib import Path
from types import SimpleNamespace

from core.storage import (
    Point,
    StorageCore,
    StorageRuntimeConfig,
    aggregate_points,
    query_aggregates,
)


def make_config(tmp_path, **overrides):
    """Return ``StorageRuntimeConfig`` rooted under ``tmp_path`` with optional overrides."""
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


def rows(results):
    """Serialize aggregate results to row dicts."""
    return [result.to_row() for result in results]


def test_aggregate_points_groups_by_parameter_id():
    """``aggregate_points`` groups samples by parameter id within the window."""
    results = aggregate_points(
        [
            Point(timestamp_ns=100, parameter_id=2, value=10.0),
            Point(timestamp_ns=101, parameter_id=1, value=1.0),
            Point(timestamp_ns=102, parameter_id=2, value=20.0),
            Point(timestamp_ns=103, parameter_id=1, value=5.0),
        ],
        start_ts_ns=100,
        end_ts_ns=110,
    )

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 110,
            "parameter_id": 1,
            "count": 2,
            "min": 1.0,
            "max": 5.0,
            "avg": 3.0,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 110,
            "parameter_id": 2,
            "count": 2,
            "min": 10.0,
            "max": 20.0,
            "avg": 15.0,
        },
    ]


def test_aggregate_points_filters_requested_parameter_ids():
    """Only requested ``parameter_ids`` appear in the output."""
    results = aggregate_points(
        [
            Point(timestamp_ns=100, parameter_id=1, value=1.0),
            Point(timestamp_ns=100, parameter_id=2, value=2.0),
            Point(timestamp_ns=100, parameter_id=3, value=3.0),
        ],
        start_ts_ns=100,
        end_ts_ns=101,
        parameter_ids={1, 3},
    )

    assert [result.parameter_id for result in results] == [1, 3]
    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 101,
            "parameter_id": 1,
            "count": 1,
            "min": 1.0,
            "max": 1.0,
            "avg": 1.0,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 101,
            "parameter_id": 3,
            "count": 1,
            "min": 3.0,
            "max": 3.0,
            "avg": 3.0,
        },
    ]


def test_aggregate_points_returns_empty_requested_parameters():
    """Missing parameters still yield rows with zero count and null aggregates."""
    results = aggregate_points(
        [],
        start_ts_ns=100,
        end_ts_ns=200,
        parameter_ids={2, 1},
    )

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 200,
            "parameter_id": 1,
            "count": 0,
            "min": None,
            "max": None,
            "avg": None,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 200,
            "parameter_id": 2,
            "count": 0,
            "min": None,
            "max": None,
            "avg": None,
        },
    ]


def test_query_aggregates_reads_memtable_only(tmp_path):
    """``query_aggregates`` reads rows still held in the memtable."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))
    storage.append_rows(
        [
            (100, [(1, 1.0), (2, 10.0)]),
            (110, [(1, 5.0), (2, 20.0)]),
        ]
    )

    results = query_aggregates(storage, 100, 120, None)

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 1,
            "count": 2,
            "min": 1.0,
            "max": 5.0,
            "avg": 3.0,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 2,
            "count": 2,
            "min": 10.0,
            "max": 20.0,
            "avg": 15.0,
        },
    ]


def test_query_aggregates_reads_sstable_only(tmp_path):
    """After flush, aggregates come from on-disk SSTables."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))
    storage.append_rows([(100, [(1, 1.0)]), (110, [(1, 3.0)])])
    storage.flush()

    results = query_aggregates(storage, 100, 120, {1})

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 1,
            "count": 2,
            "min": 1.0,
            "max": 3.0,
            "avg": 2.0,
        }
    ]


def test_aggregate_range_reads_mixed_memtable_and_sstable(tmp_path):
    """``aggregate_range`` merges flushed data with recent memtable rows."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))
    storage.append_rows([(100, [(1, 1.0)]), (110, [(1, 3.0)])])
    storage.flush()
    storage.append_rows([(120, [(1, 5.0)]), (130, [(2, 20.0)])])

    results = storage.aggregate_range(100, 140, None)

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 140,
            "parameter_id": 1,
            "count": 3,
            "min": 1.0,
            "max": 5.0,
            "avg": 3.0,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 140,
            "parameter_id": 2,
            "count": 1,
            "min": 20.0,
            "max": 20.0,
            "avg": 20.0,
        },
    ]


def test_aggregate_range_uses_query_range_newest_wins(tmp_path):
    """Overlapping SST layers resolve to the newest value per timestamp."""
    storage = StorageCore(
        config=make_config(tmp_path, flush_max_rows=100, compaction_min_tables=10)
    )

    storage.append_rows([(100, [(1, 1.0), (2, 2.0)]), (110, [(1, 11.0)])])
    storage.flush()
    storage.append_rows([(100, [(1, 10.0)]), (105, [(3, 3.0)])])
    storage.flush()
    storage.append_rows([(100, [(1, 100.0)]), (105, [(3, 30.0)])])

    results = storage.aggregate_range(95, 111, {1, 3})

    assert rows(results) == [
        {
            "start_ts_ns": 95,
            "end_ts_ns": 111,
            "parameter_id": 1,
            "count": 2,
            "min": 11.0,
            "max": 100.0,
            "avg": 55.5,
        },
        {
            "start_ts_ns": 95,
            "end_ts_ns": 111,
            "parameter_id": 3,
            "count": 1,
            "min": 30.0,
            "max": 30.0,
            "avg": 30.0,
        },
    ]


def test_aggregate_range_respects_half_open_time_interval(tmp_path):
    """Timestamps at ``end_ts_ns`` are excluded (half-open interval)."""
    storage = StorageCore(config=make_config(tmp_path, flush_max_rows=100))
    storage.append_rows(
        [
            (100, [(1, 1.0)]),
            (110, [(1, 3.0)]),
            (120, [(1, 100.0)]),
        ]
    )

    results = storage.aggregate_range(100, 120, {1})

    assert rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 1,
            "count": 2,
            "min": 1.0,
            "max": 3.0,
            "avg": 2.0,
        }
    ]


def test_aggregate_api_is_available_from_public_storage_package():
    """Aggregate helpers and ``aggregate_range`` are exposed on ``core.storage``."""
    import core.storage as storage_package

    config = StorageRuntimeConfig.from_settings(
        SimpleNamespace(
            data_dir=Path("ignored"),
            storage=SimpleNamespace(),
        )
    )

    assert storage_package.aggregate_points is aggregate_points
    assert storage_package.query_aggregates is query_aggregates
    assert hasattr(storage_package.StorageCore, "aggregate_range")
    assert config.flush_max_rows == 10_000
