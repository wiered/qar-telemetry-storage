"""Tests for startup recovery when manifests or SSTables are inconsistent."""

import json
from pathlib import Path
from typing import Any

from core.storage import StorageCore, StorageRuntimeConfig


def make_config(tmp_path: Path, **overrides: Any) -> StorageRuntimeConfig:
    """Return a StorageRuntimeConfig rooted at ``tmp_path / "storage"``.

    Args:
        tmp_path: Pytest temporary directory fixture.
        **overrides: Keyword overrides for StorageRuntimeConfig fields.

    Returns:
        Runtime configuration pointing at ``tmp_path / "storage"``.
    """
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 100),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 2),
        compaction_min_tables=overrides.get("compaction_min_tables", 10),
        sstable_format=overrides.get("sstable_format", "v2_timeseries"),
        cleanup_temp_on_startup=overrides.get("cleanup_temp_on_startup", True),
        quarantine_dir_name=overrides.get("quarantine_dir_name", "quarantine"),
    )


def test_recovery_uses_backup_manifest_when_primary_is_inconsistent(tmp_path):
    """Load from backup manifest when the primary manifest is internally inconsistent."""
    config = make_config(tmp_path)
    storage = StorageCore(config=config)
    storage.append_rows([(10, [(1, 1.0)])])
    storage.flush()
    storage.close()

    manifest_path = config.data_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["next_table_id"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    recovered = StorageCore(config=config)
    points = recovered.query_range(0, 20, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 10, "parameter_id": 1, "value": 1.0}
    ]
    assert (config.quarantine_dir / "manifest.json.invalid-primary-manifest").exists()
    assert recovered.stats_snapshot().quarantined_files >= 1
    recovered.close()


def test_recovery_rebuilds_manifest_from_valid_sstables_and_quarantines_invalid_files(
    tmp_path,
):
    """Rebuild manifest from SSTables and quarantine corrupt manifests or SST files."""
    config = make_config(tmp_path, flush_max_rows=1, compaction_min_tables=10)
    storage = StorageCore(config=config)
    storage.append_rows([(10, [(1, 1.0)])])
    storage.append_rows([(20, [(2, 2.0)])])
    storage.close()

    first_sst = config.sst_dir / "sst_00000000000000000001.sst"
    second_sst = config.sst_dir / "sst_00000000000000000002.sst"
    second_bytes = second_sst.read_bytes()
    second_sst.write_bytes(second_bytes[:-8])
    (config.data_dir / "manifest.json").write_text("{bad json", encoding="utf-8")
    (config.data_dir / "manifest.json.bak").write_text("{bad json", encoding="utf-8")

    recovered = StorageCore(config=config)
    points = recovered.query_range(0, 30, None)

    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 10, "parameter_id": 1, "value": 1.0}
    ]
    quarantine_names = {path.name for path in config.quarantine_dir.iterdir()}
    assert "manifest.json.invalid-primary-manifest" in quarantine_names
    assert "manifest.json.bak.invalid-backup-manifest" in quarantine_names
    assert "sst_00000000000000000002.sst.invalid-sstable" in quarantine_names
    assert recovered.stats_snapshot().manifest_rebuild_count == 1
    recovered.close()
    assert first_sst.exists()


def test_recovery_quarantines_orphans_and_temp_leftovers(tmp_path):
    """Move orphan SSTables and stray ``.tmp`` startup files into quarantine."""
    config = make_config(tmp_path)
    storage = StorageCore(config=config)
    storage.append_rows([(10, [(1, 1.0)])])
    storage.flush()
    storage.close()

    source_file = next(config.sst_dir.glob("*.sst"))
    orphan = config.sst_dir / "sst_99999999999999999999.sst"
    orphan.write_bytes(source_file.read_bytes())
    (config.data_dir / "manifest.json.tmp").write_text("tmp", encoding="utf-8")
    (config.data_dir / "manifest.json.bak.tmp").write_text("tmp", encoding="utf-8")
    (config.sst_dir / "leftover.sst.tmp").write_bytes(b"tmp")

    recovered = StorageCore(config=config)

    quarantine_names = {path.name for path in config.quarantine_dir.iterdir()}
    assert "manifest.json.tmp.startup-temp" in quarantine_names
    assert "manifest.json.bak.tmp.startup-temp" in quarantine_names
    assert "leftover.sst.tmp.startup-temp" in quarantine_names
    assert "sst_99999999999999999999.sst.orphan-sstable" in quarantine_names
    recovered.close()
