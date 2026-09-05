"""Tests for core storage row/point models and metadata helpers."""

from core.storage import ManifestTableEntry, MemtableSnapshot, Point


def test_point_sort_key_orders_by_timestamp_then_parameter():
    """Sorting by ``sort_key`` orders timestamp then parameter id."""
    points = [
        Point(timestamp_ns=2, parameter_id=1, value=9.0),
        Point(timestamp_ns=1, parameter_id=2, value=5.0),
        Point(timestamp_ns=1, parameter_id=1, value=7.0),
    ]

    ordered = sorted(points, key=lambda point: point.sort_key)

    assert ordered == [
        Point(timestamp_ns=1, parameter_id=1, value=7.0),
        Point(timestamp_ns=1, parameter_id=2, value=5.0),
        Point(timestamp_ns=2, parameter_id=1, value=9.0),
    ]


def test_row_and_point_remain_distinct_models():
    """Raw ingest rows stay tuples; points are structured models."""
    row = (100, [(1, 1.5), (2, 2.5)])
    point = Point(timestamp_ns=100, parameter_id=1, value=1.5)

    assert isinstance(row, tuple)
    assert row[0] == point.timestamp_ns
    assert row != point


def test_snapshot_and_manifest_entry_hold_expected_metadata():
    """Snapshot emptiness and manifest overlap filtering behave as expected."""
    snapshot = MemtableSnapshot(
        rows_count=2,
        points_count=3,
        approx_bytes=72,
        points=(
            Point(timestamp_ns=10, parameter_id=1, value=1.0),
            Point(timestamp_ns=11, parameter_id=2, value=2.0),
            Point(timestamp_ns=12, parameter_id=3, value=3.0),
        ),
    )
    entry = ManifestTableEntry(
        table_id=7,
        file_name="sst_00000000000000000007.sst",
        point_count=3,
        rows_count=2,
        block_count=1,
        min_timestamp_ns=10,
        max_timestamp_ns=12,
        min_parameter_id=1,
        max_parameter_id=3,
    )

    assert not snapshot.is_empty
    assert entry.overlaps_query(10, 13, {1, 3})
    assert not entry.overlaps_query(13, 20, None)
