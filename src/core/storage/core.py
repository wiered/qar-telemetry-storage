"""Storage engine core orchestration.

This module contains ``StorageCore`` that coordinates ingestion, querying,
manifest recovery, memtable flush, compaction, and runtime statistics.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from logging import getLogger
from pathlib import Path
import threading
import time
from typing import TYPE_CHECKING, Any

from .compaction import compact_tables, merge_runs, select_candidates
from .config import StorageRuntimeConfig
from .manifest import MANIFEST_VERSION, ManifestStore
from .memtable import Memtable
from .models import (
    ManifestData,
    ManifestTableEntry,
    MemtableSnapshot,
    Point,
    Row,
    StorageStats,
)
from .sstable import SSTableReader, SSTableWriter

if TYPE_CHECKING:
    from .analysis import AggregateResult

logger = getLogger(__name__)


class StorageCore:
    """Coordinate in-memory and on-disk storage lifecycle.

    The class manages write buffering (memtable), immutable SSTables, manifest
    consistency, startup recovery, background maintenance, and query execution.

    Args:
        config: Explicit runtime config. If omitted, ``settings`` must be
            provided and ``StorageRuntimeConfig.from_settings(settings)`` is used.
        settings: Application settings object used when ``config`` is ``None``.
            Ignored if ``config`` is provided.
        recover: Whether to recover persisted state on startup. Default is True.

    Raises:
        ValueError: If both ``config`` and ``settings`` are ``None``.
    """

    def __init__(
        self,
        *,
        config: StorageRuntimeConfig | None = None,
        settings: Any | None = None,
        recover: bool = True,
    ) -> None:
        if config is None:
            if settings is None:
                raise ValueError("Must provide either 'config' or 'settings'")
            config = StorageRuntimeConfig.from_settings(settings)

        self.config = config
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.sst_dir.mkdir(parents=True, exist_ok=True)
        self.config.quarantine_dir.mkdir(parents=True, exist_ok=True)

        self._state_lock = threading.RLock()
        self._maintenance_lock = threading.Lock()
        self._manifest_store = ManifestStore(self.config.data_dir)
        self._sstable_writer = SSTableWriter(
            self.config.sst_dir,
            self.config.sstable_block_max_points,
            sstable_format=self.config.sstable_format,
        )
        self._memtable = Memtable()
        self._tables: list[ManifestTableEntry] = []
        self._next_table_id = 1
        self._closed = False
        self._pending_flushes: deque[tuple[int, MemtableSnapshot]] = deque()
        self._stats_data: dict[str, int] = {}

        if recover:
            self.recover()

    def append_rows(self, rows: list[Row]) -> None:
        """Append rows to active memtable and trigger flush if needed.

        Args:
            rows: Input rows to append.
        """
        if not rows:
            return

        should_drain = False
        with self._state_lock:
            self._ensure_open_locked()
            self._memtable.append_rows(rows)
            if self._memtable.should_flush(self.config):
                self._enqueue_current_memtable_flush_locked()
                should_drain = True

        if should_drain:
            self._drain_maintenance()

    def query_range(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
    ) -> list[Point]:
        """Query points in half-open time interval with optional id filter.

        Args:
            start_ts_ns: Inclusive range start timestamp in nanoseconds.
            end_ts_ns: Exclusive range end timestamp in nanoseconds.
            parameter_ids: Optional set of parameter ids to include.

        Returns:
            Merged points from memtable and SSTables ordered by merge policy.

        See also: ``merge_runs`` in ``compaction`` for duplicate-resolution semantics across sources.
        """
        with self._state_lock:
            self._ensure_open_locked()
            memtable_snapshot = self._memtable.snapshot()
            tables_snapshot = tuple(self._tables)

        local_stats = {
            "files_considered": 0,
            "files_pruned": 0,
            "files_opened": 0,
            "blocks_considered": 0,
            "blocks_pruned": 0,
            "blocks_scanned": 0,
            "points_decoded": 0,
            "points_returned": 0,
        }

        sources: list[tuple[int, Iterable[Point]]] = []
        if not memtable_snapshot.is_empty:
            memtable_priority = (
                max((table.table_id for table in tables_snapshot), default=0) + 1
            )
            sources.append(
                (
                    memtable_priority,
                    (
                        point
                        for point in memtable_snapshot.points
                        if start_ts_ns <= point.timestamp_ns < end_ts_ns
                        and (
                            parameter_ids is None or point.parameter_id in parameter_ids
                        )
                    ),
                )
            )

        for table in sorted(
            tables_snapshot, key=lambda entry: entry.table_id, reverse=True
        ):
            local_stats["files_considered"] += 1
            if not table.overlaps_query(start_ts_ns, end_ts_ns, parameter_ids):
                local_stats["files_pruned"] += 1
                continue

            sources.append(
                (
                    table.table_id,
                    self._iter_table_points(
                        table,
                        start_ts_ns,
                        end_ts_ns,
                        parameter_ids,
                        local_stats,
                    ),
                )
            )

        points = list(merge_runs(sources))
        local_stats["points_returned"] = len(points)
        self._add_stats(**local_stats)
        return points

    def aggregate_range(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
    ) -> list[AggregateResult]:
        """Compute aggregates over a range using analysis layer.

        Args:
            start_ts_ns: Inclusive range start timestamp in nanoseconds.
            end_ts_ns: Exclusive range end timestamp in nanoseconds.
            parameter_ids: Optional set of parameter ids to include.

        Returns:
            Aggregate rows produced by ``analysis.query_aggregates``.
        """
        from .analysis import query_aggregates

        return query_aggregates(self, start_ts_ns, end_ts_ns, parameter_ids)

    def flush(self) -> None:
        """Flush current memtable to SSTable if it contains data."""
        should_drain = False
        with self._state_lock:
            self._ensure_open_locked()
            if self._memtable.is_empty:
                return
            self._enqueue_current_memtable_flush_locked()
            should_drain = True

        if should_drain:
            self._drain_maintenance()

    def compact(self) -> bool:
        """Run maintenance cycle and report whether compaction happened.

        Returns:
            ``True`` when at least one compaction task was executed.
        """
        with self._state_lock:
            self._ensure_open_locked()
        return self._drain_maintenance()

    def close(self) -> None:
        """Close storage and persist pending in-memory data.

        If already closed, this method is idempotent and returns immediately without
        running the drain process again.

        On the first call, this method sets the closed flag. If the memtable is
        non-empty, it enqueues the memtable for flush (using the same rotation path
        as ``flush()``), and then drains the maintenance queue until idle, including
        pending flushes and compaction.

        Successful completion indicates the drain finished without raising exceptions
        (executing the same SSTable write and manifest update steps as a normal
        flush). Exceptions during those steps propagate; storage remains closed, and
        subsequent calls to ``close()`` will not retry failed work. Note that raw
        persistence is still subject to OS filesystem buffering.

        """
        with self._maintenance_lock:
            with self._state_lock:
                if self._closed:
                    return
                self._closed = True
                if not self._memtable.is_empty:
                    self._enqueue_current_memtable_flush_locked()
            self._drain_maintenance_locked()

    def recover(self) -> None:
        """Recover manifest and table set from disk.

        Recovery tries primary manifest, then backup manifest, and finally
        manifest rebuild from discovered SSTables. Broken files are quarantined.

        Raises:
            RuntimeError: If recovery starts with non-empty memtable.
        """
        started_ns = time.perf_counter_ns()
        quarantined_count = 0
        manifest_rebuilt = False

        with self._maintenance_lock:
            with self._state_lock:
                self._ensure_open_locked()
                if not self._memtable.is_empty:
                    raise RuntimeError("recover() requires an empty memtable")
                self._pending_flushes.clear()
                self._tables = []
                self._next_table_id = 1

            quarantined_count += self._cleanup_startup_leftovers()

            manifest: ManifestData | None = None
            recovered_from = "primary"
            try:
                manifest = self._load_and_validate_manifest(
                    self._manifest_store.load_primary
                )
            except Exception as exc:
                recovered_from = "backup"
                logger.warning("primary manifest recovery failed: %s", exc)
                quarantined_count += self._quarantine_path(
                    self._manifest_store.path,
                    "invalid-primary-manifest",
                )
                try:
                    manifest = self._load_and_validate_manifest(
                        self._manifest_store.load_backup
                    )
                except Exception as backup_exc:
                    recovered_from = "rebuild"
                    logger.warning("backup manifest recovery failed: %s", backup_exc)
                    quarantined_count += self._quarantine_path(
                        self._manifest_store.backup_path,
                        "invalid-backup-manifest",
                    )
                    manifest, rebuild_quarantined = (
                        self._rebuild_manifest_from_sstables()
                    )
                    quarantined_count += rebuild_quarantined
                    manifest_rebuilt = True

            assert manifest is not None
            orphan_quarantined = self._quarantine_orphan_sstables(manifest)
            quarantined_count += orphan_quarantined
            if (
                recovered_from != "primary"
                or orphan_quarantined > 0
                or manifest_rebuilt
            ):
                self._manifest_store.save(manifest)

            with self._state_lock:
                self._tables = list(manifest.tables)
                self._next_table_id = max(1, int(manifest.next_table_id))

        elapsed_ns = time.perf_counter_ns() - started_ns
        self._add_stats(
            recovery_count=1,
            recovery_duration_ns=elapsed_ns,
            quarantined_files=quarantined_count,
            manifest_rebuild_count=1 if manifest_rebuilt else 0,
        )
        logger.info(
            "storage recovery completed: source=%s tables=%s quarantined=%s rebuilt=%s duration_ms=%.3f",
            recovered_from,
            len(manifest.tables),
            quarantined_count,
            manifest_rebuilt,
            elapsed_ns / 1_000_000,
        )

    def stats_snapshot(self) -> StorageStats:
        """Return current accumulated runtime statistics snapshot."""
        with self._state_lock:
            return StorageStats(**self._stats_data)

    def _enqueue_current_memtable_flush_locked(self) -> None:
        """Rotate memtable and enqueue snapshot for flush.

        Note:
            Caller must hold ``_state_lock``.
        """
        snapshot = self._rotate_memtable_locked()
        table_id = self._reserve_table_id_locked()
        self._pending_flushes.append((table_id, snapshot))

    def _rotate_memtable_locked(self) -> MemtableSnapshot:
        """Swap current memtable with a fresh one and return snapshot.

        Note:
            Caller must hold ``_state_lock``.
        """
        snapshot = self._memtable.snapshot()
        self._memtable = Memtable()
        return snapshot

    def _reserve_table_id_locked(self) -> int:
        """Reserve and return next table id.

        Note:
            Caller must hold ``_state_lock``.
        """
        table_id = self._next_table_id
        self._next_table_id += 1
        return table_id

    def _drain_maintenance(self) -> bool:
        """Run maintenance under maintenance lock.

        Returns:
            ``True`` when at least one compaction was performed.
        """
        with self._maintenance_lock:
            return self._drain_maintenance_locked()

    def _drain_maintenance_locked(self) -> bool:
        """Process queued flushes and compaction tasks until idle.

        Returns:
            ``True`` when at least one compaction was performed.
        """
        compaction_performed = False
        while True:
            pending_flush: tuple[int, MemtableSnapshot] | None = None
            compaction_task: tuple[int, tuple[ManifestTableEntry, ...]] | None = None

            with self._state_lock:
                if self._pending_flushes:
                    pending_flush = self._pending_flushes.popleft()
                else:
                    candidates = select_candidates(
                        self._tables,
                        self.config.compaction_min_tables,
                    )
                    if candidates:
                        compaction_task = (
                            self._reserve_table_id_locked(),
                            tuple(candidates),
                        )

            if pending_flush is not None:
                self._perform_flush(*pending_flush)
                continue
            if compaction_task is not None:
                self._perform_compaction(*compaction_task)
                compaction_performed = True
                continue
            return compaction_performed

    def _perform_flush(self, table_id: int, snapshot: MemtableSnapshot) -> None:
        """Write a memtable snapshot as a new SSTable and update manifest.

        Args:
            table_id: Reserved table id for new SSTable.
            snapshot: Immutable memtable snapshot to flush.
        """
        if snapshot.is_empty:
            return

        metadata = self._sstable_writer.write_snapshot(table_id, snapshot)
        table_entry = ManifestTableEntry.from_file_metadata(metadata)
        file_size = (self.config.sst_dir / metadata.file_name).stat().st_size
        with self._state_lock:
            manifest = ManifestData(
                version=MANIFEST_VERSION,
                next_table_id=self._next_table_id,
                tables=tuple(self._tables) + (table_entry,),
            )

        self._manifest_store.save(manifest)

        with self._state_lock:
            self._tables.append(table_entry)
        self._add_stats(
            flush_count=1,
            flush_rows=snapshot.rows_count,
            flush_points=snapshot.points_count,
            bytes_written=file_size,
        )

    def _perform_compaction(
        self,
        table_id: int,
        candidates: tuple[ManifestTableEntry, ...],
    ) -> None:
        """Compact candidate tables into one table and update manifest.

        Args:
            table_id: Reserved table id for compacted SSTable.
            candidates: SSTable manifest entries selected for compaction.
        """
        started_ns = time.perf_counter_ns()
        compacted_table = compact_tables(
            sst_dir=self.config.sst_dir,
            writer=self._sstable_writer,
            table_id=table_id,
            tables=candidates,
        )
        file_size = (self.config.sst_dir / compacted_table.file_name).stat().st_size
        candidate_ids = {table.table_id for table in candidates}
        with self._state_lock:
            surviving_tables = tuple(
                table for table in self._tables if table.table_id not in candidate_ids
            )
            manifest = ManifestData(
                version=MANIFEST_VERSION,
                next_table_id=self._next_table_id,
                tables=surviving_tables + (compacted_table,),
            )

        self._manifest_store.save(manifest)

        with self._state_lock:
            self._tables = list(manifest.tables)

        for table in candidates:
            try:
                (self.config.sst_dir / table.file_name).unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                logger.warning(
                    "failed to delete compacted SSTable %s: %s", table.file_name, exc
                )

        self._add_stats(
            compaction_count=1,
            compaction_duration_ns=time.perf_counter_ns() - started_ns,
            compaction_rewrite_points=compacted_table.point_count,
            compaction_rewrite_bytes=file_size,
            bytes_written=file_size,
        )

    def _load_and_validate_manifest(self, loader: Any) -> ManifestData:
        """Load manifest with provided loader and validate consistency.

        Args:
            loader: Callable that returns ``ManifestData``.

        Returns:
            Validated manifest instance.
        """
        manifest = loader()
        manifest.validate()
        self._validate_manifest_tables(manifest)
        return manifest

    def _validate_manifest_tables(self, manifest: ManifestData) -> None:
        """Validate that manifest table metadata matches SSTable files.

        Args:
            manifest: Manifest to validate against on-disk SSTables.

        Raises:
            FileNotFoundError: If manifest references missing SSTable.
            ValueError: If table metadata differs from SSTable metadata.
        """
        for table in manifest.tables:
            file_path = self.config.sst_dir / table.file_name
            if not file_path.exists():
                raise FileNotFoundError(
                    f"missing SSTable referenced by manifest: {table.file_name}"
                )
            version, file_metadata, _blocks = SSTableReader.inspect(file_path)
            manifest_metadata = table.to_dict()
            file_metadata_dict = ManifestTableEntry.from_file_metadata(
                file_metadata
            ).to_dict()
            if manifest_metadata != file_metadata_dict:
                raise ValueError(
                    f"manifest metadata mismatch for {table.file_name}: "
                    f"manifest={manifest_metadata}, file={file_metadata_dict}, version={version}"
                )

    def _cleanup_startup_leftovers(self) -> int:
        """Quarantine temporary leftovers left from interrupted writes.

        Returns:
            Number of quarantined files.
        """
        if not self.config.cleanup_temp_on_startup:
            return 0

        quarantined = 0
        for path in (
            self._manifest_store.tmp_path,
            self._manifest_store.backup_tmp_path,
        ):
            quarantined += self._quarantine_path(path, "startup-temp")
        for path in self.config.sst_dir.glob("*.sst.tmp"):
            quarantined += self._quarantine_path(path, "startup-temp")
        return quarantined

    def _rebuild_manifest_from_sstables(self) -> tuple[ManifestData, int]:
        """Rebuild manifest by scanning valid SSTables in storage directory.

        Returns:
            Tuple of rebuilt manifest and number of quarantined files.

        Raises:
            ValueError: If rebuilt manifest metadata is internally inconsistent.
        """
        valid_entries: list[ManifestTableEntry] = []
        quarantined = 0
        seen_table_ids: set[int] = set()
        seen_file_names: set[str] = set()

        for file_path in sorted(self.config.sst_dir.glob("*.sst")):
            try:
                _version, metadata, _blocks = SSTableReader.inspect(file_path)
                entry = ManifestTableEntry.from_file_metadata(metadata)
                entry.validate()
                if (
                    entry.table_id in seen_table_ids
                    or entry.file_name in seen_file_names
                ):
                    raise ValueError(
                        "duplicate table metadata discovered during rebuild"
                    )
                seen_table_ids.add(entry.table_id)
                seen_file_names.add(entry.file_name)
                valid_entries.append(entry)
            except Exception as exc:
                logger.warning(
                    "quarantining invalid SSTable %s during rebuild: %s",
                    file_path.name,
                    exc,
                )
                quarantined += self._quarantine_path(file_path, "invalid-sstable")

        valid_entries.sort(key=lambda entry: entry.table_id)
        manifest = ManifestData(
            version=MANIFEST_VERSION,
            next_table_id=(valid_entries[-1].table_id + 1) if valid_entries else 1,
            tables=tuple(valid_entries),
        )
        manifest.validate()
        logger.info(
            "rebuilt manifest from SSTables: tables=%s next_table_id=%s",
            len(valid_entries),
            manifest.next_table_id,
        )
        return (manifest, quarantined)

    def _quarantine_orphan_sstables(self, manifest: ManifestData) -> int:
        """Quarantine SSTables that are not referenced by manifest.

        Args:
            manifest: Active manifest used as source of truth.

        Returns:
            Number of quarantined orphan SSTables.
        """
        active_files = {table.file_name for table in manifest.tables}
        quarantined = 0
        for file_path in self.config.sst_dir.glob("*.sst"):
            if file_path.name not in active_files:
                logger.warning(
                    "quarantining orphan SSTable not present in manifest: %s",
                    file_path.name,
                )
                quarantined += self._quarantine_path(file_path, "orphan-sstable")
        return quarantined

    def _quarantine_path(self, path: Path, reason: str) -> int:
        """Move file to quarantine directory with reason suffix.

        Args:
            path: Source file path to quarantine.
            reason: Reason token added to quarantine file name.

        Returns:
            ``1`` when file was quarantined, otherwise ``0``.
        """
        if not path.exists():
            return 0

        target = self.config.quarantine_dir / f"{path.name}.{reason}"
        counter = 1
        while target.exists():
            target = self.config.quarantine_dir / f"{path.name}.{reason}.{counter}"
            counter += 1

        try:
            path.replace(target)
        except OSError as exc:
            logger.warning("failed to quarantine %s: %s", path, exc)
            return 0

        logger.warning("quarantined %s -> %s", path.name, target.name)
        return 1

    def _iter_table_points(
        self,
        table: ManifestTableEntry,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None,
        counters: dict[str, int],
    ) -> Iterable[Point]:
        """Yield filtered points from one SSTable and update query counters.

        Args:
            table: Table metadata describing source SSTable.
            start_ts_ns: Inclusive range start timestamp in nanoseconds.
            end_ts_ns: Exclusive range end timestamp in nanoseconds.
            parameter_ids: Optional set of parameter ids to include.
            counters: Mutable query counters updated during scan.

        Yields:
            Matching points produced by SSTable reader.

        Raises:
            FileNotFoundError: If the SSTable is still active but its file is missing.
        """
        try:
            reader = SSTableReader(self.config.sst_dir / table.file_name)
            counters["files_opened"] += 1
            yield from reader.iter_range(
                start_ts_ns,
                end_ts_ns,
                parameter_ids,
                counters=counters,
            )
        except FileNotFoundError:
            if not self._table_is_active(table):
                logger.debug(
                    "query skipped compacted-away SSTable snapshot: %s", table.file_name
                )
                return
            raise

    def _table_is_active(self, table: ManifestTableEntry) -> bool:
        """Check whether table is still present in current active table set.

        Args:
            table: Table metadata snapshot to check.

        Returns:
            ``True`` if table id and file name are still active.
        """
        with self._state_lock:
            return any(
                current.table_id == table.table_id
                and current.file_name == table.file_name
                for current in self._tables
            )

    def _add_stats(self, **increments: int) -> None:
        """Add integer increments to accumulated runtime stats.

        Args:
            **increments: Stat key to increment mapping.
        """
        with self._state_lock:
            for key, value in increments.items():
                self._stats_data[key] = self._stats_data.get(key, 0) + int(value)

    def _ensure_open_locked(self) -> None:
        """Ensure storage is not closed.

        Raises:
            RuntimeError: If storage has already been closed.
        """
        if self._closed:
            raise RuntimeError("storage is closed")
