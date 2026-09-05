"""Tests for SSTable read/write, pruning, and block selection."""

from core.storage import StorageRuntimeConfig
from core.storage.memtable import Memtable
from core.storage.sstable import SSTableReader, SSTableWriter


def build_snapshot():
    """Return a small sorted snapshot spanning multiple timestamps."""
    memtable = Memtable()
    memtable.append_rows(
        [
            (100, [(1, 1.0), (3, 3.0)]),
            (110, [(2, 2.0)]),
            (120, [(4, 4.0)]),
            (130, [(5, 5.0)]),
        ]
    )
    return memtable.snapshot()


def test_sstable_round_trip_and_block_metadata(tmp_path):
    """Written SSTables round-trip points and expose block metadata."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage", sstable_block_max_points=2
    )
    writer = SSTableWriter(config.sst_dir, config.sstable_block_max_points)
    snapshot = build_snapshot()

    metadata = writer.write_snapshot(1, snapshot)
    reader = SSTableReader(config.sst_dir / metadata.file_name)

    points = reader.scan_range(100, 140, None)

    assert metadata.block_count == 3
    assert metadata.point_count == 5
    assert [point.to_dict() for point in points] == [
        point.to_dict() for point in snapshot.points
    ]
    assert reader.blocks[0].min_timestamp_ns == 100
    assert reader.blocks[0].max_timestamp_ns == 100
    assert reader.blocks[-1].min_parameter_id == 5
    assert reader.blocks[-1].max_parameter_id == 5


def test_sstable_reader_prunes_by_time_and_parameter(tmp_path):
    """scan_range respects timestamp bounds and parameter filters."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage", sstable_block_max_points=2
    )
    writer = SSTableWriter(config.sst_dir, config.sstable_block_max_points)
    snapshot = build_snapshot()
    metadata = writer.write_snapshot(1, snapshot)
    reader = SSTableReader(config.sst_dir / metadata.file_name)

    points = reader.scan_range(109, 131, {4})

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 120, "parameter_id": 4, "value": 4.0}
    ]


def test_sstable_iter_range_matches_scan_range(tmp_path):
    """iter_range agrees with filtered scan semantics."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage", sstable_block_max_points=2
    )
    writer = SSTableWriter(config.sst_dir, config.sstable_block_max_points)
    snapshot = build_snapshot()
    metadata = writer.write_snapshot(1, snapshot)
    reader = SSTableReader(config.sst_dir / metadata.file_name)

    assert [point.to_dict() for point in reader.iter_range(100, 140, {1, 2, 4})] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 110, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 120, "parameter_id": 4, "value": 4.0},
    ]


def test_sstable_v2_round_trip_and_soft_block_limit(tmp_path):
    """v2 format respects soft block size and reads back correctly."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        sstable_block_max_points=2,
        sstable_format="v2_timeseries",
    )
    writer = SSTableWriter(
        config.sst_dir,
        config.sstable_block_max_points,
        sstable_format=config.sstable_format,
    )
    memtable = Memtable()
    memtable.append_rows(
        [
            (100, [(1, 1.0), (2, 2.0), (3, 3.0)]),
            (101, [(1, 4.0)]),
        ]
    )
    snapshot = memtable.snapshot()

    metadata = writer.write_snapshot(7, snapshot)
    reader = SSTableReader(config.sst_dir / metadata.file_name)

    assert reader.version == 2
    assert metadata.sstable_version == 2
    assert metadata.block_count == 2
    assert reader.blocks[0].point_count == 3
    assert reader.blocks[0].min_timestamp_ns == 100
    assert reader.blocks[0].max_timestamp_ns == 100
    assert [point.to_dict() for point in reader.scan_range(100, 102, None)] == [
        point.to_dict() for point in snapshot.points
    ]


def test_sstable_binary_search_selects_only_overlapping_blocks(tmp_path):
    """Candidate block selection skips blocks outside the query window."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        sstable_block_max_points=2,
        sstable_format="v2_timeseries",
    )
    writer = SSTableWriter(
        config.sst_dir,
        config.sstable_block_max_points,
        sstable_format=config.sstable_format,
    )
    snapshot = build_snapshot()
    metadata = writer.write_snapshot(1, snapshot)
    reader = SSTableReader(config.sst_dir / metadata.file_name)

    selected = reader._select_candidate_blocks(109, 121)

    assert [block.min_timestamp_ns for block in selected] == [110]
