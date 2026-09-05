"""Queued ingestion of FDAU frames into batched rows for storage."""

from __future__ import annotations

import time
import threading
import queue
from typing import Dict, List, Optional, Tuple, Protocol, TypedDict
from logging import getLogger

logger = getLogger(__name__)

ParamId = int
Value = float
Columns = List[Tuple[ParamId, Value]]
Row = Tuple[int, Columns]


class FramePayload(TypedDict):
    """Normalized parameter payload from FDAU frame values."""

    parameter_id: int
    value: float


class IngestFrame(TypedDict, total=False):
    """Ingest frame contract consumed by the worker queue."""

    ts_monotonic: float
    seq: int
    values: Dict[str, FramePayload]


class IngestSettings(Protocol):
    """Expected shape for ``settings.ingest``."""

    @property
    def queue_max_frames(self) -> int:
        """Maximum number of frames allowed in the ingest queue."""
        ...

    @property
    def batch_max_rows(self) -> int:
        """Maximum number of rows buffered before flush."""
        ...

    @property
    def batch_max_points(self) -> int:
        """Maximum number of points buffered before flush."""
        ...

    @property
    def batch_max_ms(self) -> int | float:
        """Maximum batch age in milliseconds before flush."""
        ...

    @property
    def overflow_policy(self) -> str:
        """Queue overflow policy name."""
        ...

    @property
    def warn_every_dropped(self) -> int:
        """Dropped-frame warning interval."""
        ...

    @property
    def idle_sleep_ms(self) -> int | float:
        """Worker idle sleep interval in milliseconds."""
        ...


class AppSettings(Protocol):
    """Minimal application settings required by ingest module."""

    @property
    def ingest(self) -> IngestSettings:
        """Ingest settings contract."""
        ...


class IngestStats(TypedDict):
    """Snapshot contract returned by ``IngestService.stats``."""

    queue_size: int
    frames_in: int
    rows_out: int
    points_out: int
    dropped_frames: int
    seq_gaps: int


class Storage(Protocol):
    """Minimal storage interface for ingest.

    In real implementation this appends rows to storage core.
    """

    def append_rows(self, rows: List[Row]) -> None:
        """Persist one or more timestamped rows."""
        ...


class StorageStub:
    """Simple storage stub that aggregates rows and points."""

    def __init__(self, print_every_rows: int = 2000) -> None:
        self._lock = threading.Lock()
        self.total_rows = 0
        self.total_points = 0
        self.print_every_rows = max(1, int(print_every_rows))
        self._last_print_ts = time.time()
        logger.debug(
            f"StorageStub initialized with print_every_rows={self.print_every_rows}"
        )

    def append_rows(self, rows: List[Row]) -> None:
        """Count rows/points and optionally log throughput."""
        if not rows:
            return
        points = sum(len(cols) for _, cols in rows)
        with self._lock:
            self.total_rows += len(rows)
            self.total_points += points
            if self.total_rows % self.print_every_rows == 0:
                now = time.time()
                dt = max(1e-9, now - self._last_print_ts)
                self._last_print_ts = now
                rps = self.print_every_rows / dt
                pps = points / dt
                logger.info(
                    f"Storage: +{len(rows)} rows, +{points} points | "
                    f"totals: rows={self.total_rows}, points={self.total_points} | "
                    f"~{rps:.1f} rows/s, ~{pps:.1f} points/s"
                )


class IngestWorker(threading.Thread):
    """Consume frames from queue, batch rows, and flush to storage."""

    def __init__(
        self,
        *,
        q: "queue.Queue[IngestFrame]",
        storage: Storage,
        settings: AppSettings,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="IngestWorker", daemon=True)
        self._q = q
        self._storage = storage
        self._settings = settings
        self._stop_event = stop_event

        self._batch: List[Row] = []
        self._batch_points = 0
        self._batch_started_monotonic: Optional[float] = None

        # metrics
        self.frames_in = 0
        self.rows_out = 0
        self.points_out = 0
        self.dropped_frames = 0
        self.seq_gaps = 0
        self._last_seq: Optional[int] = None

        logger.debug(
            f"IngestWorker initialized: queue_max={settings.ingest.queue_max_frames}, "
            f"batch_max_rows={settings.ingest.batch_max_rows}, "
            f"batch_max_points={getattr(settings.ingest, 'batch_max_points', 0)}, "
            f"batch_max_ms={settings.ingest.batch_max_ms}"
        )

    @staticmethod
    def _frame_to_row(frame: IngestFrame) -> Optional[Row]:
        """Convert one FDAU frame into a row tuple.

        Args:
            frame: Frame with optional ``ts_monotonic`` and ``values`` mapping where
                each value contains ``parameter_id`` and ``value``.

        Returns:
            Row tuple ``(t_ns, columns)`` where ``columns`` are
            ``[(param_id, value), ...]``, or ``None`` when frame has no valid points.
        """
        values = frame.get("values") or {}
        if not values:
            logger.debug("Frame has no values, skipping")
            return None

        ts_m = frame.get("ts_monotonic", None)
        if ts_m is None:
            # fallback to wall clock if missing
            ts_m = time.perf_counter()
            logger.debug("Frame missing ts_monotonic, using perf_counter")

        t_ns = int(float(ts_m) * 1_000_000_000)

        cols: List[Tuple[int, float]] = []
        malformed_count = 0
        for name, payload in values.items():
            try:
                pid = int(payload["parameter_id"])
                val = float(payload["value"])
            except Exception as e:
                malformed_count += 1
                logger.debug(f"Malformed entry in frame for parameter '{name}': {e}")
                continue
            cols.append((pid, val))

        if malformed_count > 0:
            logger.warning(f"Frame had {malformed_count} malformed parameter entries")

        if not cols:
            logger.debug("Frame converted to empty row, skipping")
            return None
        return (t_ns, cols)

    def _maybe_flush_by_time(self, now_monotonic: float) -> None:
        if not self._batch:
            return
        if self._batch_started_monotonic is None:
            self._batch_started_monotonic = now_monotonic
            return

        max_ms = float(self._settings.ingest.batch_max_ms)
        if max_ms <= 0:
            return

        elapsed_ms = (now_monotonic - self._batch_started_monotonic) * 1000.0
        if elapsed_ms >= max_ms:
            self._flush()

    def _maybe_flush_by_size(self) -> None:
        max_rows = int(self._settings.ingest.batch_max_rows)
        if max_rows > 0 and len(self._batch) >= max_rows:
            self._flush()
            return

        max_points = int(getattr(self._settings.ingest, "batch_max_points", 0))
        if max_points > 0 and self._batch_points >= max_points:
            self._flush()

    def _flush(self) -> None:
        if not self._batch:
            self._batch_started_monotonic = None
            self._batch_points = 0
            return

        rows = self._batch
        batch_size = len(rows)
        points = self._batch_points
        self._batch = []
        self._batch_points = 0
        self._batch_started_monotonic = None

        # update metrics
        self.rows_out += batch_size
        self.points_out += points

        logger.debug(f"Flushing batch: {batch_size} rows, {points} points")

        # write
        self._storage.append_rows(rows)

    def _check_seq_gap(self, frame: IngestFrame) -> None:
        seq = frame.get("seq")
        if seq is None:
            return
        try:
            seq_i = int(seq)
        except Exception:
            return
        if self._last_seq is not None and seq_i != self._last_seq + 1:
            gap_size = seq_i - self._last_seq - 1
            self.seq_gaps += 1
            logger.warning(
                f"Sequence gap detected: expected {self._last_seq + 1}, got {seq_i} "
                f"(gap size: {gap_size}, total gaps: {self.seq_gaps})"
            )
        self._last_seq = seq_i

    def run(self) -> None:
        """Drain the ingest queue until stop, batching flushes to storage."""
        idle_sleep = max(
            0.0, float(getattr(self._settings.ingest, "idle_sleep_ms", 5)) / 1000.0
        )
        logger.info("IngestWorker thread started")

        while not self._stop_event.is_set() or not self._q.empty():
            now_m = time.perf_counter()
            self._maybe_flush_by_time(now_m)

            try:
                frame = self._q.get(timeout=idle_sleep if idle_sleep > 0 else 0.01)
            except queue.Empty:
                continue

            self.frames_in += 1
            self._check_seq_gap(frame)

            row = self._frame_to_row(frame)
            if row is None:
                continue

            self._batch.append(row)
            self._batch_points += len(row[1])
            self._maybe_flush_by_size()

        # graceful shutdown flush
        logger.info("IngestWorker stopping, performing final flush")
        self._flush()
        logger.info(
            f"IngestWorker stopped: processed {self.frames_in} frames, "
            f"output {self.rows_out} rows ({self.points_out} points), "
            f"detected {self.seq_gaps} sequence gaps"
        )


class IngestService:
    """Thread-safe ingest front-door between producer and storage worker.

    Args:
        settings: Application settings with ``settings.ingest`` contract.
        storage: Storage backend implementation. Defaults to ``StorageStub``.
    """

    def __init__(
        self, *, settings: AppSettings, storage: Optional[Storage] = None
    ) -> None:
        self.settings = settings
        self.storage: Storage = storage or StorageStub()

        self._stop_event = threading.Event()
        queue_max = int(self.settings.ingest.queue_max_frames)
        self._q: "queue.Queue[IngestFrame]" = queue.Queue(maxsize=queue_max)

        self._worker = IngestWorker(
            q=self._q,
            storage=self.storage,
            settings=self.settings,
            stop_event=self._stop_event,
        )

        self.dropped_frames = 0
        self._dropped_since_warn = 0

        logger.info(
            f"IngestService initialized: queue_max={queue_max}, "
            f"overflow_policy={settings.ingest.overflow_policy}, "
            f"storage={type(self.storage).__name__}"
        )

    def start(self) -> None:
        """Start worker thread if it is not running."""
        self._stop_event.clear()
        if not self._worker.is_alive():
            self._worker.start()
            logger.info("IngestService started")
        else:
            logger.warning("IngestService start() called but worker is already alive")

    def stop(self, *, join: bool = True, timeout: Optional[float] = None) -> None:
        """Stop worker and optionally wait for graceful shutdown.

        Args:
            join: If ``True``, wait for thread termination.
            timeout: Optional timeout in seconds for ``join``.
        """
        logger.info("IngestService stop requested")
        self._stop_event.set()
        if join:
            if self._worker.ident is None:
                logger.info(
                    "IngestService stop() called before worker start; nothing to join"
                )
                return
            self._worker.join(timeout=timeout)
            if self._worker.is_alive():
                logger.warning(f"IngestWorker did not stop within timeout={timeout}")
            else:
                logger.info("IngestService stopped successfully")

    def on_frame(self, frame: IngestFrame) -> None:
        """Enqueue one producer frame using configured overflow policy.

        Args:
            frame: Raw frame received from producer callback.
        """
        policy = str(
            getattr(self.settings.ingest, "overflow_policy", "drop_newest")
        ).lower()
        warn_every = int(getattr(self.settings.ingest, "warn_every_dropped", 1000))

        if policy == "block":
            self._q.put(frame)
            return

        try:
            self._q.put_nowait(frame)
        except queue.Full:
            if policy == "drop_oldest":
                try:
                    _ = self._q.get_nowait()
                except queue.Empty:
                    pass
                try:
                    self._q.put_nowait(frame)
                    return
                except queue.Full:
                    pass

            self.dropped_frames += 1
            self._dropped_since_warn += 1
            self._worker.dropped_frames += 1

            if warn_every > 0 and self._dropped_since_warn >= warn_every:
                self._dropped_since_warn = 0
                logger.warning(
                    f"Queue full, dropped frames: total={self.dropped_frames}, "
                    f"queue_max={self.settings.ingest.queue_max_frames}, "
                    f"policy={policy}, queue_size={self._q.qsize()}"
                )

    def stats(self) -> IngestStats:
        """Return current ingest counters snapshot.

        Returns:
            Dictionary with queue size, ingest counters, drops, and sequence gaps.
        """
        w = self._worker
        stats_dict: IngestStats = {
            "queue_size": self._q.qsize(),
            "frames_in": w.frames_in,
            "rows_out": w.rows_out,
            "points_out": w.points_out,
            "dropped_frames": self.dropped_frames,
            "seq_gaps": w.seq_gaps,
        }
        logger.debug(f"IngestService stats: {stats_dict}")
        return stats_dict


if __name__ == "__main__":
    from types import SimpleNamespace

    demo_settings = SimpleNamespace(
        ingest=SimpleNamespace(
            queue_max_frames=16,
            batch_max_rows=4,
            batch_max_points=32,
            batch_max_ms=100.0,
            overflow_policy="drop_newest",
            warn_every_dropped=10,
            idle_sleep_ms=5.0,
        )
    )

    ingest = IngestService(
        settings=demo_settings, storage=StorageStub(print_every_rows=1)
    )
    ingest.start()
    ingest.on_frame(
        {
            "ts_monotonic": time.perf_counter(),
            "seq": 1,
            "values": {"demo": {"parameter_id": 1, "value": 42.0}},
        }
    )
    time.sleep(0.05)
    print(ingest.stats())
    ingest.stop()
