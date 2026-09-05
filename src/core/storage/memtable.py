"""In-memory buffering of rows and points before SSTable flush."""

from __future__ import annotations

from .config import StorageRuntimeConfig
from .models import MemtableSnapshot, Point, Row

POINT_SIZE_BYTES = 24


class Memtable:
    """In-memory accumulator for rows before flushing to SSTable.

    New instances start empty with zeroed counters.
    """

    def __init__(self) -> None:
        self.rows_count = 0
        self.points_count = 0
        self.approx_bytes = 0
        self._points: list[Point] = []

    @property
    def is_empty(self) -> bool:
        """Return whether the memtable currently has no points."""
        return self.points_count == 0

    def append_rows(self, rows: list[Row]) -> None:
        """Append incoming rows and update aggregate counters.

        Args:
            rows: Rows in ``(timestamp_ns, [(parameter_id, value), ...])`` format.
        """
        for timestamp_ns, values in rows:
            self.rows_count += 1
            for parameter_id, value in values:
                self._points.append(
                    Point(
                        timestamp_ns=int(timestamp_ns),
                        parameter_id=int(parameter_id),
                        value=float(value),
                    )
                )
                self.points_count += 1
        self.approx_bytes = self.points_count * POINT_SIZE_BYTES

    def snapshot(self) -> MemtableSnapshot:
        """Build an immutable sorted view of current memtable contents.

        Returns:
            Snapshot with points sorted by storage key.
        """
        return MemtableSnapshot(
            rows_count=self.rows_count,
            points_count=self.points_count,
            approx_bytes=self.approx_bytes,
            points=tuple(sorted(self._points, key=lambda point: point.sort_key)),
        )

    def clear(self) -> None:
        """Reset all counters and remove buffered points."""
        self.rows_count = 0
        self.points_count = 0
        self.approx_bytes = 0
        self._points = []

    def should_flush(self, config: StorageRuntimeConfig) -> bool:
        """Check whether memtable exceeds configured flush thresholds.

        Args:
            config: Runtime storage limits and flush policy.

        Returns:
            ``True`` when flush should be triggered, otherwise ``False``.
        """
        return config.should_flush(
            self.rows_count, self.points_count, self.approx_bytes
        )
