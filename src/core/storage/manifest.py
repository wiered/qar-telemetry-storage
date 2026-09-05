"""Atomic read/write of manifest JSON backing SSTable catalog metadata."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .models import ManifestData

MANIFEST_VERSION = 1


class ManifestStore:
    """Persistent storage for primary and backup manifest files.

    Args:
        root_dir: Storage root directory containing manifest files.
    """

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = Path(root_dir)
        self._root_dir.mkdir(parents=True, exist_ok=True)
        self._path = self._root_dir / "manifest.json"
        self._backup_path = self._root_dir / "manifest.json.bak"
        self._tmp_path = self._root_dir / "manifest.json.tmp"
        self._backup_tmp_path = self._root_dir / "manifest.json.bak.tmp"

    @property
    def path(self) -> Path:
        """Return path to the primary manifest file."""
        return self._path

    @property
    def backup_path(self) -> Path:
        """Return path to the backup manifest file."""
        return self._backup_path

    @property
    def tmp_path(self) -> Path:
        """Return path to temporary primary manifest file."""
        return self._tmp_path

    @property
    def backup_tmp_path(self) -> Path:
        """Return path to temporary backup manifest file."""
        return self._backup_tmp_path

    def load_primary(self) -> ManifestData:
        """Load manifest data from the primary manifest file.

        Returns:
            Parsed and validated manifest data.
        """
        return self._load_path(self._path)

    def load_backup(self) -> ManifestData:
        """Load manifest data from the backup manifest file.

        Returns:
            Parsed and validated manifest data.
        """
        return self._load_path(self._backup_path)

    def load(self) -> ManifestData:
        """Load manifest data from the primary manifest file.

        Returns:
            Parsed and validated manifest data.
        """
        return self.load_primary()

    def save(self, manifest: ManifestData) -> None:
        """Persist manifest atomically to primary and backup files.

        Args:
            manifest: Manifest model to validate and serialize.
        """
        manifest.validate()
        payload = json.dumps(
            manifest.to_dict(),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        self._write_atomic(self._tmp_path, self._path, payload)
        self._write_atomic(self._backup_tmp_path, self._backup_path, payload)
        self._fsync_directory()

    def _load_path(self, path: Path) -> ManifestData:
        """Load and validate manifest data from a specific path.

        Args:
            path: File path to read manifest JSON from.

        Returns:
            Parsed manifest data, or default manifest if file is missing.

        Raises:
            ValueError: If file cannot be read or parsed as valid manifest JSON.
        """
        if not path.exists():
            return ManifestData(version=MANIFEST_VERSION, next_table_id=1, tables=())

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"failed to load manifest {path.name}: {exc}") from exc

        manifest = ManifestData.from_dict(payload)
        manifest.validate()
        return manifest

    @staticmethod
    def _write_atomic(tmp_path: Path, final_path: Path, payload: str) -> None:
        """Write payload to temp file, fsync it, then atomically replace target.

        Args:
            tmp_path: Temporary file path used for atomic write.
            final_path: Final destination file path.
            payload: Serialized JSON payload to write.
        """
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, final_path)

    def _fsync_directory(self) -> None:
        """Fsync storage directory to reduce metadata loss after power failure."""
        try:
            fd = os.open(self._root_dir, os.O_RDONLY)
        except OSError:
            return

        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)
