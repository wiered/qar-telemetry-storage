"""Immutable dataclasses for points, SSTable metadata, manifests, and stats.

Value objects used by the storage layer for serialization boundaries (dict payloads),
query pruning (time and parameter bounds), and observability counters.

Attributes:
    Row: Type alias for encoded logical rows used by ingestion and SSTables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

Row = tuple[int, list[tuple[int, float]]]
"""Logical row: timestamp in nanoseconds followed by ``(parameter_id, value)`` pairs."""


@dataclass(frozen=True, slots=True)
class Point:
    """Single scalar measurement at a timestamp for one parameter.

    Attributes:
        timestamp_ns: Sample time in nanoseconds since epoch.
        parameter_id: Stable identifier for the measured parameter.
        value: Observed value as IEEE-754 float64.
    """

    timestamp_ns: int
    parameter_id: int
    value: float

    @property
    def sort_key(self) -> tuple[int, int]:
        """Composite key for ordering points by time then parameter id.

        Returns:
            Tuple ``(timestamp_ns, parameter_id)``.
        """
        return (self.timestamp_ns, self.parameter_id)

    def to_dict(self) -> dict[str, int | float]:
        """Serialize the point to a JSON-friendly mapping.

        Returns:
            Mapping with keys ``timestamp_ns``, ``parameter_id``, and ``value``.
        """
        return {
            "timestamp_ns": self.timestamp_ns,
            "parameter_id": self.parameter_id,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class MemtableSnapshot:
    """Frozen slice of memtable data flushed toward an SSTable.

    Attributes:
        rows_count: Number of logical rows represented.
        points_count: Total scalar points across all rows.
        approx_bytes: Rough serialized size estimate for planning limits.
        points: Materialized points in flush order.
    """

    rows_count: int
    points_count: int
    approx_bytes: int
    points: tuple[Point, ...]

    @property
    def is_empty(self) -> bool:
        """Whether the snapshot contains no points.

        Returns:
            ``True`` when ``points_count`` is zero.
        """
        return self.points_count == 0


@dataclass(frozen=True, slots=True)
class BlockMetadata:
    """Bounds and location of one contiguous block inside an SSTable file.

    Attributes:
        offset: Byte offset from the start of the file to block payload.
        size_bytes: Serialized size of the block payload in bytes.
        point_count: Number of points stored in the block.
        min_timestamp_ns: Minimum timestamp among points in the block.
        max_timestamp_ns: Maximum timestamp among points in the block.
        min_parameter_id: Minimum parameter id among points in the block.
        max_parameter_id: Maximum parameter id among points in the block.
    """

    offset: int
    size_bytes: int
    point_count: int
    min_timestamp_ns: int
    max_timestamp_ns: int
    min_parameter_id: int
    max_parameter_id: int

    def overlaps_time_range(self, start_ts_ns: int, end_ts_ns: int) -> bool:
        """Return whether the block intersects the half-open time window.

        Args:
            start_ts_ns: Window start in nanoseconds (inclusive).
            end_ts_ns: Window end in nanoseconds (exclusive).

        Returns:
            ``True`` if any timestamps in the block may fall into the window.
        """
        return (
            self.max_timestamp_ns >= start_ts_ns and self.min_timestamp_ns < end_ts_ns
        )

    def may_contain_parameter_ids(self, parameter_ids: set[int] | None) -> bool:
        """Whether the block can contain any of the given parameter ids.

        Args:
            parameter_ids: Restrict set, or ``None`` to skip filtering.

        Returns:
            ``True`` when ``parameter_ids`` is ``None`` or ranges overlap.
        """
        if parameter_ids is None:
            return True
        return any(
            self.min_parameter_id <= parameter_id <= self.max_parameter_id
            for parameter_id in parameter_ids
        )

    def overlaps_query(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
    ) -> bool:
        """Combine time and optional parameter pruning predicates.

        Args:
            start_ts_ns: Query window start (nanoseconds, inclusive).
            end_ts_ns: Query window end (nanoseconds, exclusive).
            parameter_ids: Optional parameter filter.

        Returns:
            ``True`` if the block might satisfy both predicates.
        """
        return self.overlaps_time_range(
            start_ts_ns, end_ts_ns
        ) and self.may_contain_parameter_ids(parameter_ids)

    def validate(self) -> None:
        """Check internal consistency of numeric bounds.

        Raises:
            ValueError: When offsets, counts, or min/max pairs are invalid.
        """
        if self.offset < 0:
            raise ValueError("block offset must be non-negative")
        if self.size_bytes <= 0:
            raise ValueError("block size_bytes must be positive")
        if self.point_count <= 0:
            raise ValueError("block point_count must be positive")
        if self.min_timestamp_ns > self.max_timestamp_ns:
            raise ValueError("block timestamp bounds are inconsistent")
        if self.min_parameter_id > self.max_parameter_id:
            raise ValueError("block parameter bounds are inconsistent")

    def to_dict(self) -> dict[str, int]:
        """Serialize block metadata for JSON or manifest sidecars.

        Returns:
            Mapping with keys matching constructor fields.
        """
        return {
            "offset": self.offset,
            "size_bytes": self.size_bytes,
            "point_count": self.point_count,
            "min_timestamp_ns": self.min_timestamp_ns,
            "max_timestamp_ns": self.max_timestamp_ns,
            "min_parameter_id": self.min_parameter_id,
            "max_parameter_id": self.max_parameter_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, int]) -> "BlockMetadata":
        """Build metadata from a decoded dict payload.

        Args:
            payload: Mapping produced by :meth:`to_dict` or compatible readers.

        Returns:
            Validated :class:`BlockMetadata` instance (call :meth:`validate` if needed).
        """
        return cls(
            offset=int(payload["offset"]),
            size_bytes=int(payload["size_bytes"]),
            point_count=int(payload["point_count"]),
            min_timestamp_ns=int(payload["min_timestamp_ns"]),
            max_timestamp_ns=int(payload["max_timestamp_ns"]),
            min_parameter_id=int(payload["min_parameter_id"]),
            max_parameter_id=int(payload["max_parameter_id"]),
        )


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """Aggregate bounds for one on-disk SSTable file.

    Attributes:
        table_id: Unique table identifier assigned by the manifest allocator.
        file_name: Relative path or basename of the SSTable file.
        point_count: Total points across all blocks.
        rows_count: Logical row count as written during flush.
        block_count: Number of blocks in the file.
        min_timestamp_ns: Minimum timestamp in the file.
        max_timestamp_ns: Maximum timestamp in the file.
        min_parameter_id: Minimum parameter id in the file.
        max_parameter_id: Maximum parameter id in the file.
        sstable_version: On-disk format version number.
        size_bytes: Serialized file length including headers and footer.
    """

    table_id: int
    file_name: str
    point_count: int
    rows_count: int
    block_count: int
    min_timestamp_ns: int
    max_timestamp_ns: int
    min_parameter_id: int
    max_parameter_id: int
    sstable_version: int = 1
    size_bytes: int = 0

    def overlaps_time_range(self, start_ts_ns: int, end_ts_ns: int) -> bool:
        """Return whether the file intersects the half-open time window.

        Args:
            start_ts_ns: Window start in nanoseconds (inclusive).
            end_ts_ns: Window end in nanoseconds (exclusive).

        Returns:
            ``True`` if any timestamps in the file may fall into the window.
        """
        return (
            self.max_timestamp_ns >= start_ts_ns and self.min_timestamp_ns < end_ts_ns
        )

    def may_contain_parameter_ids(self, parameter_ids: set[int] | None) -> bool:
        """Whether the file can contain any of the given parameter ids.

        Args:
            parameter_ids: Restrict set, or ``None`` to skip filtering.

        Returns:
            ``True`` when ``parameter_ids`` is ``None`` or ranges overlap.
        """
        if parameter_ids is None:
            return True
        return any(
            self.min_parameter_id <= parameter_id <= self.max_parameter_id
            for parameter_id in parameter_ids
        )

    def overlaps_query(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
    ) -> bool:
        """Combine time and optional parameter pruning predicates.

        Args:
            start_ts_ns: Query window start (nanoseconds, inclusive).
            end_ts_ns: Query window end (nanoseconds, exclusive).
            parameter_ids: Optional parameter filter.

        Returns:
            ``True`` if the file might satisfy both predicates.
        """
        return self.overlaps_time_range(
            start_ts_ns, end_ts_ns
        ) and self.may_contain_parameter_ids(parameter_ids)

    def validate(self) -> None:
        """Check file-level invariants before trusting metadata.

        Raises:
            ValueError: When identifiers, counts, or bounds are inconsistent.
        """
        if self.table_id <= 0:
            raise ValueError("table_id must be positive")
        if not self.file_name:
            raise ValueError("file_name must not be empty")
        if self.point_count <= 0:
            raise ValueError("point_count must be positive")
        if self.rows_count <= 0:
            raise ValueError("rows_count must be positive")
        if self.block_count <= 0:
            raise ValueError("block_count must be positive")
        if self.min_timestamp_ns > self.max_timestamp_ns:
            raise ValueError("file timestamp bounds are inconsistent")
        if self.min_parameter_id > self.max_parameter_id:
            raise ValueError("file parameter bounds are inconsistent")
        if self.sstable_version <= 0:
            raise ValueError("sstable_version must be positive")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")

    def to_dict(self) -> dict[str, int | str]:
        """Serialize file metadata for manifests or tooling.

        Returns:
            Mapping with keys matching stable JSON field names.
        """
        return {
            "table_id": self.table_id,
            "file_name": self.file_name,
            "point_count": self.point_count,
            "rows_count": self.rows_count,
            "block_count": self.block_count,
            "min_timestamp_ns": self.min_timestamp_ns,
            "max_timestamp_ns": self.max_timestamp_ns,
            "min_parameter_id": self.min_parameter_id,
            "max_parameter_id": self.max_parameter_id,
            "sstable_version": self.sstable_version,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, int | str]) -> "FileMetadata":
        """Parse metadata from a decoded dict payload.

        Args:
            payload: Mapping produced by :meth:`to_dict` or manifest writes.

        Returns:
            Constructed :class:`FileMetadata` (call :meth:`validate` if needed).
        """
        return cls(
            table_id=int(payload["table_id"]),
            file_name=str(payload["file_name"]),
            point_count=int(payload["point_count"]),
            rows_count=int(payload["rows_count"]),
            block_count=int(payload["block_count"]),
            min_timestamp_ns=int(payload["min_timestamp_ns"]),
            max_timestamp_ns=int(payload["max_timestamp_ns"]),
            min_parameter_id=int(payload["min_parameter_id"]),
            max_parameter_id=int(payload["max_parameter_id"]),
            sstable_version=int(payload.get("sstable_version", 1)),
            size_bytes=int(payload.get("size_bytes", 0)),
        )


@dataclass(frozen=True, slots=True)
class ManifestTableEntry:
    """One table row embedded in the manifest (same fields as :class:`FileMetadata`).

    Attributes:
        table_id: Unique table identifier.
        file_name: SSTable path segment referenced by the manifest.
        point_count: Total points in the table.
        rows_count: Logical rows stored in the table.
        block_count: Blocks inside the SSTable.
        min_timestamp_ns: Minimum timestamp covered by the table.
        max_timestamp_ns: Maximum timestamp covered by the table.
        min_parameter_id: Minimum parameter id in the table.
        max_parameter_id: Maximum parameter id in the table.
        sstable_version: On-disk format version.
        size_bytes: Serialized SSTable size on disk.
    """

    table_id: int
    file_name: str
    point_count: int
    rows_count: int
    block_count: int
    min_timestamp_ns: int
    max_timestamp_ns: int
    min_parameter_id: int
    max_parameter_id: int
    sstable_version: int = 1
    size_bytes: int = 0

    def overlaps_query(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
    ) -> bool:
        """Delegate to :class:`FileMetadata` pruning logic with identical fields.

        Args:
            start_ts_ns: Query window start (nanoseconds, inclusive).
            end_ts_ns: Query window end (nanoseconds, exclusive).
            parameter_ids: Optional parameter filter.

        Returns:
            ``True`` when the represented file might match the query.
        """
        return FileMetadata(
            table_id=self.table_id,
            file_name=self.file_name,
            point_count=self.point_count,
            rows_count=self.rows_count,
            block_count=self.block_count,
            min_timestamp_ns=self.min_timestamp_ns,
            max_timestamp_ns=self.max_timestamp_ns,
            min_parameter_id=self.min_parameter_id,
            max_parameter_id=self.max_parameter_id,
            sstable_version=self.sstable_version,
            size_bytes=self.size_bytes,
        ).overlaps_query(start_ts_ns, end_ts_ns, parameter_ids)

    def validate(self) -> None:
        """Validate using :meth:`FileMetadata.validate` on a synthetic wrapper.

        Note:
            Errors match :class:`FileMetadata` ``ValueError`` rules because the
            implementation forwards to that method without catching exceptions.
        """
        FileMetadata(
            table_id=self.table_id,
            file_name=self.file_name,
            point_count=self.point_count,
            rows_count=self.rows_count,
            block_count=self.block_count,
            min_timestamp_ns=self.min_timestamp_ns,
            max_timestamp_ns=self.max_timestamp_ns,
            min_parameter_id=self.min_parameter_id,
            max_parameter_id=self.max_parameter_id,
            sstable_version=self.sstable_version,
            size_bytes=self.size_bytes,
        ).validate()

    def to_dict(self) -> dict[str, int | str]:
        """Serialize this manifest row.

        Returns:
            Mapping compatible with :meth:`from_dict`.
        """
        return {
            "table_id": self.table_id,
            "file_name": self.file_name,
            "point_count": self.point_count,
            "rows_count": self.rows_count,
            "block_count": self.block_count,
            "min_timestamp_ns": self.min_timestamp_ns,
            "max_timestamp_ns": self.max_timestamp_ns,
            "min_parameter_id": self.min_parameter_id,
            "max_parameter_id": self.max_parameter_id,
            "sstable_version": self.sstable_version,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, int | str]) -> "ManifestTableEntry":
        """Parse a manifest table entry from decoded JSON-like data.

        Args:
            payload: Mapping with keys produced by :meth:`to_dict`.

        Returns:
            Constructed entry (call :meth:`validate` if needed).
        """
        return cls(
            table_id=int(payload["table_id"]),
            file_name=str(payload["file_name"]),
            point_count=int(payload["point_count"]),
            rows_count=int(payload["rows_count"]),
            block_count=int(payload["block_count"]),
            min_timestamp_ns=int(payload["min_timestamp_ns"]),
            max_timestamp_ns=int(payload["max_timestamp_ns"]),
            min_parameter_id=int(payload["min_parameter_id"]),
            max_parameter_id=int(payload["max_parameter_id"]),
            sstable_version=int(payload.get("sstable_version", 1)),
            size_bytes=int(payload.get("size_bytes", 0)),
        )

    @classmethod
    def from_file_metadata(cls, metadata: FileMetadata) -> "ManifestTableEntry":
        """Copy fields from :class:`FileMetadata` into a manifest row.

        Args:
            metadata: Completed SSTable header/footer aggregate.

        Returns:
            Equivalent manifest entry referencing the same file bounds.
        """
        return cls(
            table_id=metadata.table_id,
            file_name=metadata.file_name,
            point_count=metadata.point_count,
            rows_count=metadata.rows_count,
            block_count=metadata.block_count,
            min_timestamp_ns=metadata.min_timestamp_ns,
            max_timestamp_ns=metadata.max_timestamp_ns,
            min_parameter_id=metadata.min_parameter_id,
            max_parameter_id=metadata.max_parameter_id,
            sstable_version=metadata.sstable_version,
            size_bytes=metadata.size_bytes,
        )


@dataclass(frozen=True, slots=True)
class ManifestData:
    """Top-level manifest payload describing all registered SSTables.

    Attributes:
        version: Manifest schema version number.
        next_table_id: Next allocator value strictly above every ``table_id``.
        tables: Registered tables sorted by increasing ``table_id``.
    """

    version: int
    next_table_id: int
    tables: tuple[ManifestTableEntry, ...] = ()

    def validate(self) -> None:
        """Ensure manifest ordering, uniqueness, and nested row validity.

        Raises:
            ValueError: On duplicate ids, bad ordering, or invalid nested rows.
        """
        if self.version <= 0:
            raise ValueError("manifest version must be positive")
        if self.next_table_id <= 0:
            raise ValueError("manifest next_table_id must be positive")

        seen_table_ids: set[int] = set()
        seen_file_names: set[str] = set()
        previous_table_id = 0
        for table in self.tables:
            table.validate()
            if table.table_id in seen_table_ids:
                raise ValueError(f"duplicate table_id in manifest: {table.table_id}")
            if table.file_name in seen_file_names:
                raise ValueError(f"duplicate file_name in manifest: {table.file_name}")
            if table.table_id <= previous_table_id:
                raise ValueError(
                    "manifest tables must be ordered by increasing table_id"
                )
            if table.table_id >= self.next_table_id:
                raise ValueError("manifest next_table_id must exceed all table_ids")

            seen_table_ids.add(table.table_id)
            seen_file_names.add(table.file_name)
            previous_table_id = table.table_id

    def to_dict(self) -> dict[str, int | list[dict[str, int | str]]]:
        """Serialize the manifest for atomic rewrite to disk.

        Returns:
            Nested mapping suitable for JSON encoding.
        """
        return {
            "version": self.version,
            "next_table_id": self.next_table_id,
            "tables": [table.to_dict() for table in self.tables],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ManifestData":
        """Parse manifest content from loosely typed decoded JSON data.

        Args:
            payload: Outer mapping with ``version``, ``next_table_id``, and ``tables``.

        Returns:
            Parsed manifest (call :meth:`validate` before trusting invariants).
        """
        tables_payload = payload.get("tables", [])
        if not isinstance(tables_payload, list):
            raise ValueError("manifest tables must be a list")

        version_payload = payload.get("version")
        next_table_id_payload = payload.get("next_table_id")
        try:
            version = int(cast(Any, version_payload))
            next_table_id = int(cast(Any, next_table_id_payload))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "manifest version and next_table_id must be integers"
            ) from exc

        tables = tuple(
            ManifestTableEntry.from_dict(cast(dict[str, int | str], table_payload))
            for table_payload in tables_payload
            if isinstance(table_payload, dict)
        )
        return cls(
            version=version,
            next_table_id=next_table_id,
            tables=tables,
        )


@dataclass(frozen=True, slots=True)
class StorageStats:
    """Mutable-through-replacement counters for storage engine diagnostics.

    Attributes:
        files_considered: SSTables examined during query planning.
        files_pruned: SSTables skipped without opening based on metadata.
        files_opened: SSTables actually opened for decoding.
        blocks_considered: Blocks inspected after file-level pruning.
        blocks_pruned: Blocks skipped using block-level bounds.
        blocks_scanned: Blocks whose payloads were traversed.
        points_decoded: Points decoded from SSTable payloads.
        points_returned: Points emitted to the caller after filters.
        flush_count: Number of memtable flushes performed.
        flush_rows: Rows written across flushes.
        flush_points: Points written across flushes.
        bytes_written: Payload bytes written during flush/compaction.
        compaction_count: Compactions completed.
        compaction_duration_ns: Wall time spent in compaction (nanoseconds).
        compaction_rewrite_points: Points rewritten during compaction.
        compaction_rewrite_bytes: Bytes rewritten during compaction.
        recovery_count: Recovery passes executed at startup.
        recovery_duration_ns: Wall time spent in recovery (nanoseconds).
        quarantined_files: Files moved aside after corruption detection.
        manifest_rebuild_count: Times the manifest was rebuilt from SSTables.
    """

    files_considered: int = 0
    files_pruned: int = 0
    files_opened: int = 0
    blocks_considered: int = 0
    blocks_pruned: int = 0
    blocks_scanned: int = 0
    points_decoded: int = 0
    points_returned: int = 0
    flush_count: int = 0
    flush_rows: int = 0
    flush_points: int = 0
    bytes_written: int = 0
    compaction_count: int = 0
    compaction_duration_ns: int = 0
    compaction_rewrite_points: int = 0
    compaction_rewrite_bytes: int = 0
    recovery_count: int = 0
    recovery_duration_ns: int = 0
    quarantined_files: int = 0
    manifest_rebuild_count: int = 0
