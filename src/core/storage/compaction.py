"""Compaction primitives for SSTable runs.

Duplicate suppression across sources is defined by ``merge_runs``; ``StorageCore.query_range``
and ``compact_tables`` rely on that contract.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .memtable import POINT_SIZE_BYTES
from .models import ManifestTableEntry, MemtableSnapshot, Point
from .sstable import SSTableReader, SSTableWriter


@dataclass(slots=True)
class _RunState:
    """Iterator state tracked for each compaction source run.

    Attributes:
        iterator (Iterator[Point]): Points stream for one merge source.
        source_priority (int): Priority used when resolving duplicate keys.
        source_index (int): Stable index of this source in the merge heap.
        ordinal (int): Increments before each heap push from this source (first
            compared value is ``1``).
    """

    iterator: Iterator[Point]
    source_priority: int
    source_index: int
    ordinal: int = 0


def select_candidates(
    tables: Iterable[ManifestTableEntry],
    min_tables: int,
) -> tuple[ManifestTableEntry, ...]:
    """Choose tables eligible for a compaction round.

    Args:
        tables: Candidate manifest table entries.
        min_tables: Minimum number of tables required for compaction.

    Returns:
        tuple[ManifestTableEntry, ...]: Selected tables, or empty tuple when
            there are not enough candidates.

    """
    candidates = tuple(tables)
    if len(candidates) < max(1, int(min_tables)):
        return ()
    return candidates


def merge_runs(sources: Iterable[tuple[int, Iterable[Point]]]) -> Iterator[Point]:
    """Merge sorted point streams into global ``(timestamp_ns, parameter_id)`` order.

    Each iterable must yield points sorted ascending by ``(timestamp_ns,
    parameter_id)`` (as from SSTable scans).

    Note:
        Conflicting points share the same ``(timestamp_ns, parameter_id)``.
        Exactly one is emitted: the point that maximizes ``(source_priority,
        ordinal)`` lexicographically. ``source_priority`` is the integer passed
        for that source; ``ordinal`` is the monotonic counter (starting at 1)
        assigned to each point read from that source during this merge.

    Args:
        sources: ``(source_priority, points_iterable)`` pairs. Larger
            ``source_priority`` wins; ties on priority are broken by larger
            ``ordinal``.

    Yields:
        Point: Points in ascending key order, at most one per duplicate key.

    See Also:
        ``StorageCore.query_range``: assigns ``source_priority`` (memtable above
        SSTables; SSTables use ``table_id``).

    """
    heap: list[tuple[int, int, int, int, int, Point]] = []
    states: dict[int, _RunState] = {}

    def push_next(source_index: int) -> None:
        """Push next point from one source iterator to the heap.

        Args:
            source_index: Index of the source run in ``states``.

        """
        state = states[source_index]
        try:
            point = next(state.iterator)
        except StopIteration:
            return

        state.ordinal += 1
        heapq.heappush(
            heap,
            (
                point.timestamp_ns,
                point.parameter_id,
                -state.source_priority,
                source_index,
                state.ordinal,
                point,
            ),
        )

    for source_index, (source_priority, points) in enumerate(sources):
        states[source_index] = _RunState(
            iterator=iter(points),
            source_priority=int(source_priority),
            source_index=source_index,
        )
        push_next(source_index)

    while heap:
        timestamp_ns, parameter_id, _neg_priority, source_index, ordinal, point = (
            heapq.heappop(heap)
        )
        push_next(source_index)

        winner = point
        winner_priority = states[source_index].source_priority
        winner_marker = (winner_priority, ordinal)

        while heap and heap[0][0] == timestamp_ns and heap[0][1] == parameter_id:
            _, _, _, duplicate_source_index, duplicate_ordinal, duplicate_point = (
                heapq.heappop(heap)
            )
            push_next(duplicate_source_index)

            duplicate_priority = states[duplicate_source_index].source_priority
            duplicate_marker = (duplicate_priority, duplicate_ordinal)
            if duplicate_marker >= winner_marker:
                winner = duplicate_point
                winner_marker = duplicate_marker

        yield winner


def compact_tables(
    *,
    sst_dir: Path,
    writer: SSTableWriter,
    table_id: int,
    tables: Iterable[ManifestTableEntry],
) -> ManifestTableEntry:
    """Read, merge, and rewrite selected SSTables into a single table.

    Args:
        sst_dir: Directory containing SSTable files.
        writer: SSTable writer used to persist the compacted snapshot.
        table_id: Identifier to assign to the resulting SSTable.
        tables: Source tables to compact.

    Returns:
        ManifestTableEntry: Manifest entry of the newly written compacted table.

    Raises:
        ValueError: If no source tables are provided or merge produces no points.

    """
    candidates = tuple(sorted(tables, key=lambda table: table.table_id))
    if not candidates:
        raise ValueError("cannot compact an empty table set")

    merged_points = tuple(
        merge_runs(
            (
                table.table_id,
                SSTableReader(sst_dir / table.file_name).iter_range(
                    table.min_timestamp_ns,
                    table.max_timestamp_ns + 1,
                    None,
                ),
            )
            for table in candidates
        )
    )
    if not merged_points:
        raise ValueError("cannot build an empty compacted SSTable")

    rows_count = len({point.timestamp_ns for point in merged_points})
    snapshot = MemtableSnapshot(
        rows_count=rows_count,
        points_count=len(merged_points),
        approx_bytes=len(merged_points) * POINT_SIZE_BYTES,
        points=merged_points,
    )
    metadata = writer.write_snapshot(table_id, snapshot)
    return ManifestTableEntry.from_file_metadata(metadata)
