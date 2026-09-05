"""Tests for in-memory table buffering and flush thresholds."""

from pathlib import Path

from core.storage import StorageRuntimeConfig
from core.storage.memtable import Memtable, POINT_SIZE_BYTES


def make_rows() -> list[tuple[int, list[tuple[int, float]]]]:
    """Sample rows with unsorted timestamps and parameter ids."""
    return [
        (30, [(2, 3.0), (1, 1.0)]),
        (10, [(5, 5.0)]),
        (20, [(3, 2.0)]),
    ]


def test_memtable_flattens_rows_and_sorts_snapshot():
    """Snapshot points sort by timestamp then parameter id."""
    memtable = Memtable()

    memtable.append_rows(make_rows())
    snapshot = memtable.snapshot()

    assert snapshot.rows_count == 3
    assert snapshot.points_count == 4
    assert snapshot.approx_bytes == 4 * POINT_SIZE_BYTES
    assert list(snapshot.points) == [
        snapshot.points[0],
        snapshot.points[1],
        snapshot.points[2],
        snapshot.points[3],
    ]
    assert [point.sort_key for point in snapshot.points] == [
        (10, 5),
        (20, 3),
        (30, 1),
        (30, 2),
    ]


def test_memtable_clear_resets_counters():
    """``clear()`` zeros row/point counts and approximate bytes."""
    memtable = Memtable()
    memtable.append_rows(make_rows())

    memtable.clear()

    assert memtable.rows_count == 0
    assert memtable.points_count == 0
    assert memtable.approx_bytes == 0
    assert memtable.is_empty


def test_memtable_flush_trigger_by_rows():
    """Flush triggers when row count reaches ``flush_max_rows``."""
    memtable = Memtable()
    memtable.append_rows([(1, [(1, 1.0)]), (2, [(2, 2.0)])])
    config = StorageRuntimeConfig(
        data_dir=Path("ignored"),
        flush_max_rows=2,
        flush_max_points=100,
        flush_max_bytes=10_000,
    )

    assert memtable.should_flush(config)


def test_memtable_flush_trigger_by_points():
    """Flush triggers when flattened point count hits ``flush_max_points``."""
    memtable = Memtable()
    memtable.append_rows([(1, [(1, 1.0), (2, 2.0), (3, 3.0)])])
    config = StorageRuntimeConfig(
        data_dir=Path("ignored"),
        flush_max_rows=10,
        flush_max_points=3,
        flush_max_bytes=10_000,
    )

    assert memtable.should_flush(config)


def test_memtable_flush_trigger_by_bytes():
    """Flush triggers when buffered bytes reach ``flush_max_bytes``."""
    memtable = Memtable()
    memtable.append_rows([(1, [(1, 1.0), (2, 2.0)])])
    config = StorageRuntimeConfig(
        data_dir=Path("ignored"),
        flush_max_rows=10,
        flush_max_points=10,
        flush_max_bytes=2 * POINT_SIZE_BYTES,
    )

    assert memtable.should_flush(config)
