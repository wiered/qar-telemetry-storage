"""Binary SSTable files: write memtable snapshots and read points by time range.

Supports format ``v1_raw`` (dense fixed-size points) and ``v2_timeseries``
(compressed timestamp deltas, parameter dictionary, and float64 values).
"""

from __future__ import annotations

import json
import os
import struct
from bisect import bisect_left
from pathlib import Path
from typing import Iterator

from .config import SSTableFormat
from .models import BlockMetadata, FileMetadata, MemtableSnapshot, Point

POINT_STRUCT = struct.Struct("<qId")
FOOTER_STRUCT = struct.Struct("<8sIQQ")
BLOCK_V2_HEADER_STRUCT = struct.Struct("<qIIIB")
PARAMETER_ID_STRUCT = struct.Struct("<I")
FLOAT64_STRUCT = struct.Struct("<d")
MAGIC_V1 = b"QARSST1\x00"
MAGIC_V2 = b"QARSST2\x00"
VERSION_V1 = 1
VERSION_V2 = 2
SUPPORTED_VERSIONS = {VERSION_V1, VERSION_V2}


def _encode_uvarint(value: int) -> bytes:
    """Encode a non-negative integer as unsigned LEB128 (uvarint) bytes.

    Args:
        value: Integer to encode; must be >= 0.

    Returns:
        Little-endian variable-length byte sequence.

    Raises:
        ValueError: If ``value`` is negative.
    """
    if value < 0:
        raise ValueError("uvarint value must be non-negative")

    encoded = bytearray()
    current = value
    while current >= 0x80:
        encoded.append((current & 0x7F) | 0x80)
        current >>= 7
    encoded.append(current)
    return bytes(encoded)


def _decode_uvarints(data: bytes, expected_count: int) -> list[int]:
    """Decode exactly ``expected_count`` unsigned varints from ``data``.

    Args:
        data: Byte buffer containing concatenated uvarints only.
        expected_count: Number of integers to decode.

    Returns:
        List of decoded non-negative integers.

    Raises:
        ValueError: On truncated stream, length mismatch, or oversized varint.
    """
    deltas: list[int] = []
    offset = 0
    length = len(data)

    while len(deltas) < expected_count and offset < length:
        shift = 0
        value = 0
        while True:
            if offset >= length:
                raise ValueError("truncated uvarint stream")
            byte = data[offset]
            offset += 1
            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
            if shift > 63:
                raise ValueError("uvarint is too large")
        deltas.append(value)

    if len(deltas) != expected_count or offset != length:
        raise ValueError("invalid uvarint stream length")
    return deltas


class SSTableWriter:
    """Writes memtable snapshots to on-disk SSTable files (v1 or v2).

    Ensures ``sst_dir`` exists on construction.

    Args:
        sst_dir: Directory for ``sst_*.sst`` files.
        block_max_points: Target maximum points per block (at least 1).
        sstable_format: Default format for :meth:`write_snapshot` when no
            override is passed.
    """

    def __init__(
        self,
        sst_dir: Path,
        block_max_points: int,
        *,
        sstable_format: SSTableFormat = "v2_timeseries",
    ) -> None:
        self._sst_dir = Path(sst_dir)
        self._block_max_points = max(1, int(block_max_points))
        self._sstable_format = sstable_format
        self._sst_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def build_file_name(table_id: int) -> str:
        """Return the canonical SSTable file name for ``table_id``.

        Args:
            table_id: Numeric table identifier.

        Returns:
            Filename like ``sst_<zero-padded id>.sst``.
        """
        return f"sst_{table_id:020d}.sst"

    def write_snapshot(
        self,
        table_id: int,
        snapshot: MemtableSnapshot,
        *,
        sstable_format: SSTableFormat | None = None,
    ) -> FileMetadata:
        """Serialize ``snapshot`` to a new SSTable atomically (tmp then replace).

        Args:
            table_id: Used in the output filename via :meth:`build_file_name`.
            snapshot: Non-empty ordered points to persist.
            sstable_format: Optional format override; defaults to the writer's
                configured format.

        Returns:
            Metadata describing the written file (excluding ``size_bytes`` fill).

        Raises:
            ValueError: If ``snapshot`` is empty or ``sstable_format`` is unknown.
            Exception: Any error while writing the temp file is re-raised after
                best-effort removal of ``*.sst.tmp``.
        """
        if snapshot.is_empty:
            raise ValueError("cannot write an empty snapshot")

        effective_format = sstable_format or self._sstable_format
        if effective_format not in {"v1_raw", "v2_timeseries"}:
            raise ValueError(f"unsupported SSTable format: {effective_format}")

        version = VERSION_V1 if effective_format == "v1_raw" else VERSION_V2
        magic = MAGIC_V1 if version == VERSION_V1 else MAGIC_V2
        file_name = self.build_file_name(table_id)
        final_path = self._sst_dir / file_name
        tmp_path = final_path.with_suffix(".sst.tmp")
        blocks: list[BlockMetadata] = []

        try:
            with tmp_path.open("wb") as handle:
                offset = 0
                for chunk in self._iter_snapshot_blocks(snapshot):
                    block_bytes = (
                        self._encode_block_v1(chunk)
                        if version == VERSION_V1
                        else self._encode_block_v2(chunk)
                    )
                    handle.write(block_bytes)
                    blocks.append(
                        self._build_block_metadata(offset, block_bytes, chunk)
                    )
                    offset += len(block_bytes)

                file_metadata = FileMetadata(
                    table_id=table_id,
                    file_name=file_name,
                    point_count=snapshot.points_count,
                    rows_count=snapshot.rows_count,
                    block_count=len(blocks),
                    min_timestamp_ns=snapshot.points[0].timestamp_ns,
                    max_timestamp_ns=snapshot.points[-1].timestamp_ns,
                    min_parameter_id=min(
                        point.parameter_id for point in snapshot.points
                    ),
                    max_parameter_id=max(
                        point.parameter_id for point in snapshot.points
                    ),
                    sstable_version=version,
                )
                index_payload = {
                    "version": version,
                    "file_metadata": file_metadata.to_dict(),
                    "blocks": [block.to_dict() for block in blocks],
                }
                index_bytes = json.dumps(
                    index_payload,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                index_offset = offset
                handle.write(index_bytes)
                handle.write(
                    FOOTER_STRUCT.pack(magic, version, index_offset, len(index_bytes))
                )
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        os.replace(tmp_path, final_path)
        return file_metadata

    def _iter_snapshot_blocks(
        self, snapshot: MemtableSnapshot
    ) -> Iterator[tuple[Point, ...]]:
        """Split ``snapshot.points`` into contiguous blocks up to size limits.

        Args:
            snapshot: Source snapshot.

        Yields:
            Tuples of points per block; timestamps within a block may span runs
            at the boundary timestamp.
        """
        points = snapshot.points
        start_index = 0
        total_points = len(points)
        while start_index < total_points:
            end_index = min(total_points, start_index + self._block_max_points)
            boundary_timestamp = points[end_index - 1].timestamp_ns
            while (
                end_index < total_points
                and points[end_index].timestamp_ns == boundary_timestamp
            ):
                end_index += 1
            yield points[start_index:end_index]
            start_index = end_index

    @staticmethod
    def _build_block_metadata(
        offset: int,
        block_bytes: bytes,
        chunk: tuple[Point, ...],
    ) -> BlockMetadata:
        """Build index metadata for one encoded block.

        Args:
            offset: Byte offset of the block in the SSTable file.
            block_bytes: Encoded block payload.
            chunk: Points represented by ``block_bytes``.

        Returns:
            :class:`~.models.BlockMetadata` for the JSON index.
        """
        return BlockMetadata(
            offset=offset,
            size_bytes=len(block_bytes),
            point_count=len(chunk),
            min_timestamp_ns=chunk[0].timestamp_ns,
            max_timestamp_ns=chunk[-1].timestamp_ns,
            min_parameter_id=min(point.parameter_id for point in chunk),
            max_parameter_id=max(point.parameter_id for point in chunk),
        )

    @staticmethod
    def _encode_block_v1(chunk: tuple[Point, ...]) -> bytes:
        """Encode a block as dense ``timestamp_ns``, ``parameter_id``, ``value`` rows.

        Args:
            chunk: Points to encode.

        Returns:
            Raw binary block body (no separate header).
        """
        block_bytes = bytearray()
        for point in chunk:
            block_bytes.extend(
                POINT_STRUCT.pack(point.timestamp_ns, point.parameter_id, point.value)
            )
        return bytes(block_bytes)

    @staticmethod
    def _encode_block_v2(chunk: tuple[Point, ...]) -> bytes:
        """Encode a block in v2 layout (dictionary, timestamp deltas, refs, values).

        Args:
            chunk: Non-empty points; ``parameter_id`` must fit in uint32.

        Returns:
            Full v2 block bytes including header.

        Raises:
            ValueError: If any ``parameter_id`` is out of uint32 range.
        """
        parameter_dictionary: list[int] = []
        parameter_index: dict[int, int] = {}
        for point in chunk:
            if point.parameter_id < 0 or point.parameter_id > 0xFFFFFFFF:
                raise ValueError("parameter_id must fit into uint32 for SSTable v2")
            if point.parameter_id not in parameter_index:
                parameter_index[point.parameter_id] = len(parameter_dictionary)
                parameter_dictionary.append(point.parameter_id)

        dictionary_count = len(parameter_dictionary)
        if dictionary_count <= 0xFF:
            ref_width = 1
        elif dictionary_count <= 0xFFFF:
            ref_width = 2
        else:
            ref_width = 4

        delta_bytes = bytearray()
        previous_timestamp_ns = chunk[0].timestamp_ns
        for point in chunk:
            delta_bytes.extend(
                _encode_uvarint(point.timestamp_ns - previous_timestamp_ns)
            )
            previous_timestamp_ns = point.timestamp_ns

        ref_bytes = bytearray()
        for point in chunk:
            reference = parameter_index[point.parameter_id]
            ref_bytes.extend(
                reference.to_bytes(ref_width, byteorder="little", signed=False)
            )

        value_bytes = bytearray()
        for point in chunk:
            value_bytes.extend(FLOAT64_STRUCT.pack(point.value))

        dictionary_bytes = bytearray()
        for parameter_id in parameter_dictionary:
            dictionary_bytes.extend(PARAMETER_ID_STRUCT.pack(parameter_id))

        header = BLOCK_V2_HEADER_STRUCT.pack(
            chunk[0].timestamp_ns,
            len(chunk),
            dictionary_count,
            len(delta_bytes),
            ref_width,
        )
        return bytes(header + dictionary_bytes + delta_bytes + ref_bytes + value_bytes)


class SSTableReader:
    """Reads an SSTable file: footer JSON index and block-wise point iteration.

    Loads the index from ``file_path`` and caches per-block timestamp bounds.
    Instance attributes ``version``, ``file_metadata``, and ``blocks`` hold the
    parsed footer/index after initialization.

    Args:
        file_path: Path to an ``.sst`` file.
    """

    def __init__(self, file_path: Path) -> None:
        self._file_path = Path(file_path)
        self.version, self.file_metadata, self.blocks = self._load_index()
        self._block_min_timestamps = tuple(
            block.min_timestamp_ns for block in self.blocks
        )
        self._block_max_timestamps = tuple(
            block.max_timestamp_ns for block in self.blocks
        )

    @classmethod
    def inspect(
        cls,
        file_path: Path,
    ) -> tuple[int, FileMetadata, tuple[BlockMetadata, ...]]:
        """Parse ``file_path`` and return version and metadata without retaining the reader.

        Args:
            file_path: SSTable to inspect.

        Returns:
            Tuple of ``(version, file_metadata, blocks)``.
        """
        reader = cls(file_path)
        return (reader.version, reader.file_metadata, reader.blocks)

    def _load_index(self) -> tuple[int, FileMetadata, tuple[BlockMetadata, ...]]:
        """Read footer, JSON index, validate, and return parsed structures.

        Returns:
            Version, file metadata, and block metadata tuple.

        Raises:
            ValueError: On corrupt footer, invalid JSON, missing fields, or
                consistency checks failed.
        """
        file_size = self._file_path.stat().st_size
        if file_size < FOOTER_STRUCT.size:
            raise ValueError(
                f"SSTable is too small to contain a footer: {self._file_path}"
            )

        with self._file_path.open("rb") as handle:
            handle.seek(-FOOTER_STRUCT.size, os.SEEK_END)
            footer = handle.read(FOOTER_STRUCT.size)
            magic, version, index_offset, index_size = FOOTER_STRUCT.unpack(footer)
            self._validate_footer(magic, version, index_offset, index_size, file_size)

            handle.seek(index_offset)
            index_bytes = handle.read(index_size)

        try:
            payload = json.loads(index_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"invalid SSTable index payload in {self._file_path}: {exc}"
            ) from exc

        try:
            file_metadata = FileMetadata.from_dict(payload["file_metadata"])
            blocks = tuple(
                BlockMetadata.from_dict(block_payload)
                for block_payload in payload["blocks"]
            )
        except KeyError as exc:
            raise ValueError(
                f"missing SSTable index field in {self._file_path}: {exc}"
            ) from exc

        if int(payload.get("version", version)) != version:
            raise ValueError(f"SSTable index version mismatch in {self._file_path}")

        self._validate_index(
            version=version,
            file_size=file_size,
            index_offset=index_offset,
            file_metadata=file_metadata,
            blocks=blocks,
        )
        return (version, file_metadata, blocks)

    @staticmethod
    def _validate_footer(
        magic: bytes,
        version: int,
        index_offset: int,
        index_size: int,
        file_size: int,
    ) -> None:
        """Check footer fields against supported versions and file geometry.

        Args:
            magic: Magic bytes from the file footer.
            version: Declared SSTable version.
            index_offset: Byte offset where the JSON index starts.
            index_size: Length of the JSON index in bytes.
            file_size: Total file size in bytes.

        Raises:
            ValueError: If magic, version, or index bounds are invalid.
        """
        if version not in SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported SSTable version: {version}")
        expected_magic = MAGIC_V1 if version == VERSION_V1 else MAGIC_V2
        if magic != expected_magic:
            raise ValueError("invalid SSTable magic")
        if index_offset < 0 or index_size <= 0:
            raise ValueError("invalid SSTable footer bounds")
        if index_offset + index_size + FOOTER_STRUCT.size > file_size:
            raise ValueError("SSTable index points outside file bounds")

    def _validate_index(
        self,
        *,
        version: int,
        file_size: int,
        index_offset: int,
        file_metadata: FileMetadata,
        blocks: tuple[BlockMetadata, ...],
    ) -> None:
        """Cross-check metadata and blocks for ordering, counts, and timestamps.

        Args:
            version: SSTable version from the footer.
            file_size: Total file size.
            index_offset: Start of JSON index (blocks must lie before it).
            file_metadata: Parsed file-level metadata.
            blocks: Parsed block list.

        Raises:
            ValueError: On any invariant violation.
        """
        file_metadata.validate()
        if file_metadata.sstable_version not in {0, version}:
            raise ValueError("SSTable metadata version mismatch")
        if file_metadata.block_count != len(blocks):
            raise ValueError("SSTable block_count does not match index blocks")
        if file_metadata.size_bytes not in {0, file_size}:
            raise ValueError("SSTable size_bytes metadata mismatch")

        previous_end = 0
        previous_min_timestamp = None
        previous_max_timestamp = None
        point_count = 0
        for block in blocks:
            block.validate()
            if block.offset < previous_end:
                raise ValueError("SSTable block offsets overlap or are out of order")
            if block.offset + block.size_bytes > index_offset:
                raise ValueError("SSTable block points outside data region")
            if (
                previous_min_timestamp is not None
                and block.min_timestamp_ns < previous_min_timestamp
            ):
                raise ValueError("SSTable blocks are not ordered by min_timestamp_ns")
            if (
                previous_max_timestamp is not None
                and block.max_timestamp_ns < previous_max_timestamp
            ):
                raise ValueError("SSTable blocks are not ordered by max_timestamp_ns")
            previous_end = block.offset + block.size_bytes
            previous_min_timestamp = block.min_timestamp_ns
            previous_max_timestamp = block.max_timestamp_ns
            point_count += block.point_count

        if not blocks:
            raise ValueError("SSTable must contain at least one block")
        if point_count != file_metadata.point_count:
            raise ValueError("SSTable point_count does not match block totals")
        if file_metadata.min_timestamp_ns != blocks[0].min_timestamp_ns:
            raise ValueError("SSTable file min_timestamp_ns mismatch")
        if file_metadata.max_timestamp_ns != blocks[-1].max_timestamp_ns:
            raise ValueError("SSTable file max_timestamp_ns mismatch")
        if file_metadata.min_parameter_id > min(
            block.min_parameter_id for block in blocks
        ):
            raise ValueError("SSTable file min_parameter_id mismatch")
        if file_metadata.max_parameter_id < max(
            block.max_parameter_id for block in blocks
        ):
            raise ValueError("SSTable file max_parameter_id mismatch")

    def scan_range(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
        *,
        counters: dict[str, int] | None = None,
    ) -> list[Point]:
        """Return all points in ``[start_ts_ns, end_ts_ns)`` (same filters as ``iter_range``).

        Args:
            start_ts_ns: Inclusive start timestamp (nanoseconds).
            end_ts_ns: Exclusive end timestamp (nanoseconds).
            parameter_ids: If set, keep only these parameter IDs.
            counters: Optional mutable dict updated with scan statistics.

        Returns:
            Materialized list of matching points.
        """
        return list(
            self.iter_range(start_ts_ns, end_ts_ns, parameter_ids, counters=counters)
        )

    def iter_range(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_ids: set[int] | None = None,
        *,
        counters: dict[str, int] | None = None,
    ) -> Iterator[Point]:
        """Yield points in the half-open time range, pruning blocks when possible.

        Args:
            start_ts_ns: Inclusive start timestamp (nanoseconds).
            end_ts_ns: Exclusive end timestamp (nanoseconds).
            parameter_ids: If set, yield only points whose parameter ID is listed.
            counters: Optional dict with keys such as ``blocks_considered``,
                ``blocks_pruned``, ``blocks_scanned``, ``points_decoded``.

        Yields:
            Points matching the range and optional parameter filter.
        """
        if not self.file_metadata.overlaps_query(start_ts_ns, end_ts_ns, parameter_ids):
            return

        candidate_blocks = self._select_candidate_blocks(start_ts_ns, end_ts_ns)
        if counters is not None:
            counters["blocks_considered"] = counters.get("blocks_considered", 0) + len(
                self.blocks
            )
            counters["blocks_pruned"] = counters.get("blocks_pruned", 0) + (
                len(self.blocks) - len(candidate_blocks)
            )
        with self._file_path.open("rb") as handle:
            for block in candidate_blocks:
                if parameter_ids is not None and not block.may_contain_parameter_ids(
                    parameter_ids
                ):
                    if counters is not None:
                        counters["blocks_pruned"] = counters.get("blocks_pruned", 0) + 1
                    continue

                handle.seek(block.offset)
                data = handle.read(block.size_bytes)
                if counters is not None:
                    counters["blocks_scanned"] = counters.get("blocks_scanned", 0) + 1

                block_iter = (
                    self._iter_block_points_v1(data)
                    if self.version == VERSION_V1
                    else self._iter_block_points_v2(data, block)
                )
                for point in block_iter:
                    if counters is not None:
                        counters["points_decoded"] = (
                            counters.get("points_decoded", 0) + 1
                        )
                    if (
                        point.timestamp_ns < start_ts_ns
                        or point.timestamp_ns >= end_ts_ns
                    ):
                        continue
                    if (
                        parameter_ids is not None
                        and point.parameter_id not in parameter_ids
                    ):
                        continue
                    yield point

    def _select_candidate_blocks(
        self,
        start_ts_ns: int,
        end_ts_ns: int,
    ) -> tuple[BlockMetadata, ...]:
        """Return blocks that may intersect ``[start_ts_ns, end_ts_ns)`` using bisect bounds.

        Args:
            start_ts_ns: Query range start (nanoseconds).
            end_ts_ns: Query range end (nanoseconds).

        Returns:
            Subtuple of :attr:`blocks` that overlap the range by min/max timestamps.
        """
        left = bisect_left(self._block_max_timestamps, start_ts_ns)
        right = bisect_left(self._block_min_timestamps, end_ts_ns)
        if right < left:
            return ()
        return self.blocks[left:right]

    @staticmethod
    def _iter_block_points_v1(data: bytes) -> Iterator[Point]:
        """Decode v1 block payload into points.

        Args:
            data: Raw block body (multiple of packed point size).

        Yields:
            Each decoded :class:`~.models.Point`.

        Raises:
            ValueError: If ``data`` length is not aligned to the point struct size.
        """
        if len(data) % POINT_STRUCT.size != 0:
            raise ValueError("SSTable v1 block payload is misaligned")
        for offset in range(0, len(data), POINT_STRUCT.size):
            timestamp_ns, parameter_id, value = POINT_STRUCT.unpack_from(data, offset)
            yield Point(
                timestamp_ns=timestamp_ns,
                parameter_id=parameter_id,
                value=value,
            )

    @staticmethod
    def _iter_block_points_v2(data: bytes, block: BlockMetadata) -> Iterator[Point]:
        """Decode v2 block payload using header, dictionary, deltas, refs, and floats.

        Args:
            data: Full encoded block including header.
            block: Expected point count and timestamp bounds for validation.

        Yields:
            Each reconstructed point for the block.

        Raises:
            ValueError: On truncated data, count mismatches, bad ``ref_width``, or
                inconsistent timestamps vs. ``block``.
        """
        if len(data) < BLOCK_V2_HEADER_STRUCT.size:
            raise ValueError("SSTable v2 block is truncated")

        base_timestamp_ns, point_count, dictionary_count, delta_size, ref_width = (
            BLOCK_V2_HEADER_STRUCT.unpack_from(data, 0)
        )
        if point_count != block.point_count:
            raise ValueError("SSTable v2 block point_count mismatch")
        if ref_width not in {1, 2, 4}:
            raise ValueError("SSTable v2 block has invalid reference width")

        offset = BLOCK_V2_HEADER_STRUCT.size
        dictionary_size = dictionary_count * PARAMETER_ID_STRUCT.size
        delta_offset = offset + dictionary_size
        ref_offset = delta_offset + delta_size
        ref_size = point_count * ref_width
        value_offset = ref_offset + ref_size
        value_size = point_count * FLOAT64_STRUCT.size
        expected_size = value_offset + value_size
        if expected_size != len(data):
            raise ValueError("SSTable v2 block payload size mismatch")

        parameter_dictionary = [
            PARAMETER_ID_STRUCT.unpack_from(
                data, offset + (index * PARAMETER_ID_STRUCT.size)
            )[0]
            for index in range(dictionary_count)
        ]
        deltas = _decode_uvarints(data[delta_offset:ref_offset], point_count)

        timestamps: list[int] = []
        previous_timestamp_ns = base_timestamp_ns
        for delta in deltas:
            timestamp_ns = previous_timestamp_ns + delta
            timestamps.append(timestamp_ns)
            previous_timestamp_ns = timestamp_ns

        if timestamps and timestamps[0] != base_timestamp_ns:
            raise ValueError("SSTable v2 block base timestamp mismatch")
        if timestamps and timestamps[-1] != block.max_timestamp_ns:
            raise ValueError("SSTable v2 block max timestamp mismatch")

        for index in range(point_count):
            ref_start = ref_offset + (index * ref_width)
            parameter_ref = int.from_bytes(
                data[ref_start : ref_start + ref_width],
                byteorder="little",
                signed=False,
            )
            if parameter_ref >= dictionary_count:
                raise ValueError(
                    "SSTable v2 block parameter reference is out of bounds"
                )
            value = FLOAT64_STRUCT.unpack_from(
                data, value_offset + (index * FLOAT64_STRUCT.size)
            )[0]
            yield Point(
                timestamp_ns=timestamps[index],
                parameter_id=parameter_dictionary[parameter_ref],
                value=value,
            )
