"""Stress tests for concurrent append, query, flush, and compaction."""

import threading
import time

from core.storage import StorageCore, StorageRuntimeConfig


def make_config(tmp_path, **overrides):
    """Return ``StorageRuntimeConfig`` rooted under ``tmp_path`` with optional overrides."""
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 100),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 16),
        compaction_min_tables=overrides.get("compaction_min_tables", 4),
        sstable_format=overrides.get("sstable_format", "v2_timeseries"),
    )


def test_concurrent_append_and_query_preserves_newest_wins(tmp_path):
    """Parallel writes and reads still expose the latest value for a key."""
    storage = StorageCore(
        config=make_config(tmp_path, flush_max_rows=1_000, compaction_min_tables=10)
    )
    failures: list[str] = []
    stop_event = threading.Event()

    def writer() -> None:
        try:
            for value in range(50):
                storage.append_rows([(100, [(1, float(value))])])
                time.sleep(0.001)
        except Exception as exc:
            failures.append(f"writer:{exc}")
        finally:
            stop_event.set()

    def reader() -> None:
        try:
            while not stop_event.is_set():
                storage.query_range(100, 101, {1})
                time.sleep(0.001)
        except Exception as exc:
            failures.append(f"reader:{exc}")

    writer_thread = threading.Thread(target=writer, name="append-thread")
    reader_thread = threading.Thread(target=reader, name="query-thread")
    writer_thread.start()
    reader_thread.start()
    writer_thread.join()
    reader_thread.join()
    storage.flush()

    points = storage.query_range(100, 101, {1})

    assert failures == []
    assert [point.to_dict() for point in points] == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 49.0}
    ]
    storage.close()


def test_concurrent_flush_compact_and_query_do_not_raise(tmp_path):
    """Writer, reader, and compactor threads complete without exceptions."""
    config = make_config(tmp_path, flush_max_rows=1, compaction_min_tables=4)
    storage = StorageCore(config=config)
    failures: list[str] = []
    writer_done = threading.Event()

    def writer() -> None:
        try:
            for timestamp_ns in range(100, 140):
                storage.append_rows([(timestamp_ns, [(1, float(timestamp_ns))])])
                time.sleep(0.001)
        except Exception as exc:
            failures.append(f"writer:{exc}")
        finally:
            writer_done.set()

    def reader() -> None:
        try:
            while not writer_done.is_set():
                storage.query_range(100, 200, {1})
                time.sleep(0.001)
        except Exception as exc:
            failures.append(f"reader:{exc}")

    def compactor() -> None:
        try:
            while not writer_done.is_set():
                storage.compact()
                time.sleep(0.002)
        except Exception as exc:
            failures.append(f"compactor:{exc}")

    threads = [
        threading.Thread(target=writer, name="writer"),
        threading.Thread(target=reader, name="reader"),
        threading.Thread(target=compactor, name="compactor"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    storage.close()
    recovered = StorageCore(config=config)
    points = recovered.query_range(100, 200, {1})

    assert failures == []
    assert len(points) == 40
    assert points[0].to_dict() == {
        "timestamp_ns": 100,
        "parameter_id": 1,
        "value": 100.0,
    }
    assert points[-1].to_dict() == {
        "timestamp_ns": 139,
        "parameter_id": 1,
        "value": 139.0,
    }
    recovered.close()
