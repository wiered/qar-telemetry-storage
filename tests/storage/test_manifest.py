"""Tests for manifest persistence and storage recovery."""

from core.storage import (
    ManifestData,
    ManifestTableEntry,
    StorageCore,
    StorageRuntimeConfig,
)
from core.storage.manifest import ManifestStore


def build_manifest() -> ManifestData:
    """Return a fixed manifest payload used across manifest tests."""
    return ManifestData(
        version=1,
        next_table_id=3,
        tables=(
            ManifestTableEntry(
                table_id=1,
                file_name="sst_00000000000000000001.sst",
                point_count=4,
                rows_count=2,
                block_count=1,
                min_timestamp_ns=10,
                max_timestamp_ns=20,
                min_parameter_id=1,
                max_parameter_id=2,
            ),
        ),
    )


def test_manifest_save_and_load_round_trip(tmp_path):
    """Saving then loading restores an identical manifest."""
    store = ManifestStore(tmp_path / "storage")
    manifest = build_manifest()

    store.save(manifest)
    loaded = store.load()

    assert loaded == manifest


def test_manifest_save_is_atomic_and_tmp_file_is_not_left_behind(tmp_path):
    """Atomic save leaves no stray ``.tmp`` alongside the manifest."""
    store = ManifestStore(tmp_path / "storage")

    store.save(build_manifest())

    assert store.path.exists()
    assert not (store.path.parent / "manifest.json.tmp").exists()


def test_storage_core_recovers_tables_listed_in_manifest(tmp_path):
    """Reopened storage sees SST tables registered in the manifest."""
    config = StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=100,
        flush_max_points=100,
        flush_max_bytes=100_000,
    )
    storage = StorageCore(config=config)
    storage.append_rows([(10, [(1, 1.0)]), (20, [(2, 2.0)])])
    storage.flush()
    storage.close()

    recovered = StorageCore(config=config)
    points = recovered.query_range(0, 100, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 10, "parameter_id": 1, "value": 1.0},
        {"timestamp_ns": 20, "parameter_id": 2, "value": 2.0},
    ]
    recovered.close()
