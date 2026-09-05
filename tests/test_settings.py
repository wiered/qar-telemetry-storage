"""Tests for ``Settings`` loading and nested ``STORAGE__`` environment overrides."""

from typing import Any, cast

from settings import Settings


def test_settings_support_nested_storage_env_overrides(monkeypatch):
    """``STORAGE__*`` env vars map onto nested ``storage`` fields."""
    monkeypatch.setenv("STORAGE__SSTABLE_FORMAT", "v1_raw")
    monkeypatch.setenv("STORAGE__SSTABLE_BLOCK_MAX_POINTS", "512")
    monkeypatch.setenv("STORAGE__CLEANUP_TEMP_ON_STARTUP", "false")
    monkeypatch.setenv("STORAGE__QUARANTINE_DIR_NAME", "bad-files")

    settings_cls = cast(Any, Settings)
    configured = settings_cls(_env_file=None)

    assert configured.storage.sstable_format == "v1_raw"
    assert configured.storage.sstable_block_max_points == 512
    assert configured.storage.cleanup_temp_on_startup is False
    assert configured.storage.quarantine_dir_name == "bad-files"
