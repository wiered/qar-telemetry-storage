"""Tests for ``core.ingest`` frame conversion and ``IngestService`` batching."""

import queue
import threading
import time
from typing import cast

import core.ingest as ingest_module
import pytest
from core.ingest import IngestFrame, IngestService, StorageStub, IngestWorker
from settings import settings


class RecordingStorage:
    """Captures batches passed to ``append_rows`` for assertions."""

    def __init__(self):
        self.appended = []

    def append_rows(self, rows):
        """Record ``rows`` as a list copy."""
        self.appended.append(list(rows))


def make_frame(seq=0, ts_monotonic=1.25, values=None):
    """Build a synthetic ingest frame dict for tests."""
    payload = {
        "IAS": {"parameter_id": 1, "value": 123.4},
        "TAS": {"parameter_id": 2, "value": 456.7},
    }
    if values is not None:
        payload = values
    return {
        "seq": seq,
        "ts_monotonic": ts_monotonic,
        "values": payload,
    }


def test_frame_to_row_converts_values():
    """``_frame_to_row`` maps values to nanoseconds timestamp and parameter tuples."""
    row = IngestWorker._frame_to_row(make_frame())

    assert row is not None
    t_ns, cols = row
    assert t_ns == 1_250_000_000
    assert cols == [(1, 123.4), (2, 456.7)]


def test_frame_to_row_skips_empty_values():
    """Empty ``values`` yields no row."""
    assert IngestWorker._frame_to_row(make_frame(values={})) is None


def test_frame_to_row_uses_perf_counter_when_ts_missing(monkeypatch):
    """Missing ``ts_monotonic`` uses ``perf_counter`` for nanoseconds."""
    monkeypatch.setattr(ingest_module.time, "perf_counter", lambda: 42.5)

    frame = make_frame()
    frame.pop("ts_monotonic")

    row = IngestWorker._frame_to_row(frame)

    assert row is not None
    t_ns, _ = row
    assert t_ns == 42_500_000_000


def test_frame_to_row_skips_malformed_entries_and_keeps_valid_ones(caplog):
    """Malformed entries are skipped; valid columns remain and warnings log."""
    caplog.set_level("WARNING", logger="core.ingest")

    row = IngestWorker._frame_to_row(
        make_frame(
            values={
                "IAS": {"parameter_id": 1, "value": 123.4},
                "BROKEN": {"parameter_id": "oops", "value": object()},
            }
        )
    )

    assert row is not None
    _, cols = row
    assert cols == [(1, 123.4)]
    assert "malformed parameter entries" in caplog.text


def test_frame_to_row_returns_none_if_all_entries_are_malformed(caplog):
    """All-invalid ``values`` returns ``None``."""
    caplog.set_level("WARNING", logger="core.ingest")

    row = IngestWorker._frame_to_row(
        make_frame(
            values={
                "BROKEN": {"parameter_id": "oops", "value": "nan?"},
            }
        )
    )

    assert row is None
    assert "malformed parameter entries" in caplog.text


def test_ingest_service_records_stats(monkeypatch):
    """Initial stats report empty queue and zero frames."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")

    storage = StorageStub(print_every_rows=1000)
    service = IngestService(settings=settings, storage=storage)

    assert service.stats()["queue_size"] == 0
    assert service.stats()["frames_in"] == 0


def test_ingest_service_stop_before_start_is_safe(monkeypatch):
    """``stop`` without ``start`` clears queue without error."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")

    service = IngestService(settings=settings, storage=RecordingStorage())

    service.stop(join=True, timeout=0.1)

    assert service.stats()["queue_size"] == 0


def test_ingest_service_on_frame_and_worker_flush(monkeypatch):
    """Frames enqueue, worker flushes rows on batch size, stats update."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 2)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 1000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.start()

    service.on_frame(make_frame(seq=0))
    service.on_frame(make_frame(seq=1))

    service.stop(join=True, timeout=2)

    assert storage.appended
    assert sum(len(batch) for batch in storage.appended) == 2
    stats = service.stats()
    assert stats["frames_in"] >= 2
    assert stats["rows_out"] >= 2


def test_ingest_service_flushes_batch_by_time(monkeypatch):
    """``batch_max_ms`` triggers flush before row limit."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.start()

    service.on_frame(make_frame(seq=0))

    deadline = time.time() + 1.0
    while not storage.appended and time.time() < deadline:
        time.sleep(0.01)

    service.stop(join=True, timeout=2)

    assert storage.appended == [[IngestWorker._frame_to_row(make_frame(seq=0))]]


def test_ingest_service_flushes_batch_by_points(monkeypatch):
    """``batch_max_points`` caps points per flush across frames."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 3)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.start()

    frame_0 = make_frame(seq=0)
    frame_1 = make_frame(seq=1)
    service.on_frame(frame_0)
    service.on_frame(frame_1)

    deadline = time.time() + 1.0
    while not storage.appended and time.time() < deadline:
        time.sleep(0.01)

    service.stop(join=True, timeout=2)

    assert storage.appended == [
        [
            IngestWorker._frame_to_row(frame_0),
            IngestWorker._frame_to_row(frame_1),
        ]
    ]
    stats = service.stats()
    assert stats["rows_out"] == 2
    assert stats["points_out"] == 4


def test_ingest_service_ignores_non_positive_batch_max_points(monkeypatch):
    """``batch_max_points`` zero disables point-based flush until stop."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.start()

    frame_0 = make_frame(seq=0)
    frame_1 = make_frame(seq=1)
    service.on_frame(frame_0)
    service.on_frame(frame_1)

    deadline = time.time() + 1.0
    while service.stats()["frames_in"] < 2 and time.time() < deadline:
        time.sleep(0.01)

    assert storage.appended == []

    service.stop(join=True, timeout=2)

    assert storage.appended == [
        [
            IngestWorker._frame_to_row(frame_0),
            IngestWorker._frame_to_row(frame_1),
        ]
    ]
    stats = service.stats()
    assert stats["rows_out"] == 2
    assert stats["points_out"] == 4


def test_ingest_service_drop_newest_overflow(monkeypatch):
    """Single-slot queue with ``drop_newest`` may drop the incoming frame."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 1)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)

    service.on_frame(make_frame(seq=0))
    service.on_frame(make_frame(seq=1))

    assert service.dropped_frames in (0, 1)


def test_ingest_service_drop_oldest_keeps_latest_frame(monkeypatch):
    """``drop_oldest`` evicts earlier frame so the queue holds the latest seq."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 1)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_oldest")

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)

    service.on_frame(make_frame(seq=0))
    service.on_frame(make_frame(seq=1))

    remaining = service._q.get_nowait()

    assert remaining["seq"] == 1
    assert service.dropped_frames == 0


def test_ingest_service_block_policy_waits_until_queue_has_space(monkeypatch):
    """``block`` stalls producers until the worker drains the queue."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 1)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "block")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.on_frame(make_frame(seq=0))

    producer_done = threading.Event()

    def push_second_frame():
        service.on_frame(make_frame(seq=1))
        producer_done.set()

    producer = threading.Thread(target=push_second_frame, daemon=True)
    producer.start()

    time.sleep(0.05)
    assert not producer_done.is_set()

    service.start()
    producer.join(timeout=1)
    service.stop(join=True, timeout=2)

    assert producer_done.is_set()
    assert sum(len(batch) for batch in storage.appended) == 2


def test_ingest_service_stop_flushes_partial_batch_and_tracks_seq_gap(monkeypatch):
    """Stop flushes pending rows; non-contiguous seq increments gap counter."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    storage = RecordingStorage()
    service = IngestService(settings=settings, storage=storage)
    service.start()

    service.on_frame(make_frame(seq=10))
    service.on_frame(make_frame(seq=12))

    service.stop(join=True, timeout=2)

    assert storage.appended == [
        [
            IngestWorker._frame_to_row(make_frame(seq=10)),
            IngestWorker._frame_to_row(make_frame(seq=12)),
        ]
    ]
    stats = service.stats()
    assert stats["rows_out"] == 2
    assert stats["seq_gaps"] == 1


def test_ingest_service_start_while_running_only_warns(monkeypatch, caplog):
    """Second ``start`` while running logs a warning."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)
    caplog.set_level("WARNING", logger="core.ingest")

    service = IngestService(settings=settings, storage=RecordingStorage())
    service.start()
    service.start()
    service.stop(join=True, timeout=2)

    assert "worker is already alive" in caplog.text


def test_ingest_service_cannot_be_restarted_after_stop(monkeypatch):
    """``start`` after ``stop`` raises ``RuntimeError``."""
    monkeypatch.setattr(settings.ingest, "queue_max_frames", 10)
    monkeypatch.setattr(settings.ingest, "batch_max_rows", 100)
    monkeypatch.setattr(settings.ingest, "batch_max_points", 0)
    monkeypatch.setattr(settings.ingest, "batch_max_ms", 10_000)
    monkeypatch.setattr(settings.ingest, "overflow_policy", "drop_newest")
    monkeypatch.setattr(settings.ingest, "idle_sleep_ms", 1)

    service = IngestService(settings=settings, storage=RecordingStorage())
    service.start()
    service.stop(join=True, timeout=2)

    with pytest.raises(RuntimeError, match="started once"):
        service.start()


def test_check_seq_gap_ignores_missing_and_non_numeric_values():
    """Gap logic ignores bad/missing seq; first numeric seq seeds state."""
    worker = IngestWorker(
        q=queue.Queue(),
        storage=RecordingStorage(),
        settings=settings,
        stop_event=threading.Event(),
    )

    worker._check_seq_gap(cast(IngestFrame, {}))
    worker._check_seq_gap(cast(IngestFrame, {"seq": "oops"}))
    worker._check_seq_gap({"seq": 10})

    assert worker.seq_gaps == 0
    assert worker._last_seq == 10


def test_check_seq_gap_counts_duplicate_and_backwards_sequence_as_gap():
    """Duplicate or decreasing seq counts as a gap."""
    worker = IngestWorker(
        q=queue.Queue(),
        storage=RecordingStorage(),
        settings=settings,
        stop_event=threading.Event(),
    )

    worker._check_seq_gap({"seq": 10})
    worker._check_seq_gap({"seq": 10})
    worker._check_seq_gap({"seq": 9})

    assert worker.seq_gaps == 2
    assert worker._last_seq == 9
