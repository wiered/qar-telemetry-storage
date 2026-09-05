"""Tests for SSTable compaction (manual, automatic, and shutdown flush)."""

from core.storage import StorageCore, StorageRuntimeConfig


def make_config(tmp_path, **overrides):
    """Return ``StorageRuntimeConfig`` rooted under ``tmp_path`` with optional overrides."""
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 1),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 2),
        compaction_min_tables=overrides.get("compaction_min_tables", 4),
    )


def test_manual_compaction_rewrites_tables_and_preserves_query_results(tmp_path):
    """``compact()`` merges SSTables, replaces files, and keeps query results stable."""
    write_config = make_config(tmp_path, compaction_min_tables=10)
    storage = StorageCore(config=write_config)

    storage.append_rows([(100, [(1, 1.0), (2, 2.0)])])
    storage.append_rows([(100, [(1, 10.0)]), (110, [(3, 3.0)])])
    storage.append_rows([(90, [(4, 4.0)])])
    assert storage.compact() is False
    storage.append_rows([(110, [(3, 30.0)]), (120, [(5, 5.0)])])
    storage.close()

    config = make_config(tmp_path, compaction_min_tables=4)
    storage = StorageCore(config=config)
    before = [point.to_dict() for point in storage.query_range(0, 200, None)]
    old_files = sorted(path.name for path in config.sst_dir.glob("*.sst"))
    assert storage.compact() is True

    after = [point.to_dict() for point in storage.query_range(0, 200, None)]
    remaining_files = sorted(path.name for path in config.sst_dir.glob("*.sst"))

    assert before == [
        {"timestamp_ns": 90, "parameter_id": 4, "value": 4.0},
        {"timestamp_ns": 100, "parameter_id": 1, "value": 10.0},
        {"timestamp_ns": 100, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 110, "parameter_id": 3, "value": 30.0},
        {"timestamp_ns": 120, "parameter_id": 5, "value": 5.0},
    ]
    assert after == before
    assert len(storage._tables) == 1
    assert len(remaining_files) == 1
    assert remaining_files[0] not in old_files
    for file_name in old_files:
        assert not (config.sst_dir / file_name).exists()

    storage.close()
    recovered = StorageCore(config=config)
    recovered_points = [
        point.to_dict() for point in recovered.query_range(0, 200, None)
    ]
    assert recovered_points == before
    recovered.close()


def test_auto_compaction_runs_after_flush_threshold(tmp_path):
    """Enough flushes trigger compaction so multiple SSTs collapse to one."""
    config = make_config(tmp_path, compaction_min_tables=4)
    storage = StorageCore(config=config)

    storage.append_rows([(100, [(1, 1.0)])])
    storage.append_rows([(101, [(2, 2.0)])])
    storage.append_rows([(102, [(3, 3.0)])])
    storage.append_rows([(103, [(4, 4.0)])])

    assert len(storage._tables) == 1
    assert storage.compact() is False
    assert [point.to_dict() for point in storage.query_range(100, 104, None)] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 101, "parameter_id": 2, "value": 2.0},
        {"timestamp_ns": 102, "parameter_id": 3, "value": 3.0},
        {"timestamp_ns": 103, "parameter_id": 4, "value": 4.0},
    ]


def test_close_flushes_pending_memtable_without_forced_compaction(tmp_path):
    """``close()`` persists memtable data without needing compaction_min_tables."""
    config = make_config(tmp_path, flush_max_rows=100, compaction_min_tables=10)
    storage = StorageCore(config=config)

    storage.append_rows([(100, [(1, 1.0)]), (110, [(2, 2.0)])])
    storage.close()

    recovered = StorageCore(config=config)

    assert len(recovered._tables) == 1
    assert [point.to_dict() for point in recovered.query_range(0, 200, None)] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 110, "parameter_id": 2, "value": 2.0},
    ]
    recovered.close()
