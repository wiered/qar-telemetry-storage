"""MVP acceptance tests for the Storage Core user scenarios."""

from __future__ import annotations

import csv
import threading
import time
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from core.fdau import FDAUUnit
from core.flt import FLTData, FLTLayout
from core.ingest import IngestFrame, IngestService
from core.storage import (
    StorageCore,
    StorageRuntimeConfig,
    aggregates_to_rows,
    points_to_rows,
    write_aggregates_csv,
    write_points_csv,
)


def make_storage_config(tmp_path: Path, **overrides: Any) -> StorageRuntimeConfig:
    """Build MVP storage config rooted under the pytest temp directory."""
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 100),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 2),
        compaction_min_tables=overrides.get("compaction_min_tables", 10),
        sstable_format=overrides.get("sstable_format", "v2_timeseries"),
    )


def make_mvp_flt() -> FLTData:
    """Return the minimal FLT schedule used by MVP acceptance tests."""
    return FLTData(
        major_frame_sec=1,
        minor_frames=4,
        description="MVP acceptance FLT",
        layout=[
            FLTLayout(
                name="IAS",
                description="Indicated airspeed",
                parameter_id=1,
                unit="kt",
                word=1,
                minor_frames=[1, 2, 3, 4],
                hz=4,
                type="float",
            ),
            FLTLayout(
                name="TAS",
                description="True airspeed",
                parameter_id=2,
                unit="kt",
                word=2,
                minor_frames=[1, 3],
                hz=2,
                type="float",
            ),
            FLTLayout(
                name="LandingGear",
                description="Landing gear state",
                parameter_id=3,
                unit="bool",
                word=3,
                minor_frames=[4],
                hz=1,
                type="discrete",
            ),
        ],
    )


def wait_until(predicate, timeout: float = 1.0, interval: float = 0.01) -> None:
    """Wait briefly for the ingest worker thread to satisfy ``predicate``."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate()


def point_rows(points):
    """Serialize storage points to dictionaries."""
    return [point.to_dict() for point in points]


def aggregate_rows(results):
    """Serialize aggregate results to dictionaries."""
    return [result.to_row() for result in results]


def make_ingest_settings(
    *,
    batch_max_rows: int = 100,
    batch_max_points: int = 6,
    batch_max_ms: float = 10_000,
) -> SimpleNamespace:
    """Build the settings object expected by ``IngestService``."""
    return SimpleNamespace(
        ingest=SimpleNamespace(
            queue_max_frames=16,
            batch_max_rows=batch_max_rows,
            batch_max_points=batch_max_points,
            batch_max_ms=batch_max_ms,
            overflow_policy="drop_newest",
            warn_every_dropped=1000,
            idle_sleep_ms=1,
        )
    )


def make_fdau_frame(
    *,
    seq: int,
    ts_monotonic: float,
    major_frame: int,
    tick: int,
    values: dict[str, float],
) -> IngestFrame:
    """Build an FDAU-compatible frame payload for ingest."""
    flt_by_name = {layout.name: layout for layout in make_mvp_flt().layout}
    return cast(
        IngestFrame,
        {
            "seq": seq,
            "ts": 1_700_000_000.0 + ts_monotonic,
            "ts_monotonic": ts_monotonic,
            "major_frame": major_frame,
            "tick": tick,
            "values": {
                name: {
                    "parameter_id": flt_by_name[name].parameter_id,
                    "value": value,
                }
                for name, value in values.items()
            },
        },
    )


def read_csv(text: str) -> tuple[list[str] | None, list[dict[str, str]]]:
    """Read CSV text through the standard ``csv.DictReader``."""
    reader = csv.DictReader(StringIO(text))
    fieldnames = list(reader.fieldnames) if reader.fieldnames is not None else None
    return fieldnames, list(reader)


def test_mvp_fdau_ingest_storage_smoke_path(tmp_path):
    """FDAU-compatible frames travel through ingest batching into StorageCore."""
    storage = StorageCore(config=make_storage_config(tmp_path, flush_max_rows=100))
    service = IngestService(settings=make_ingest_settings(), storage=storage)
    fdau = FDAUUnit(make_mvp_flt(), stress_mode=True, seed=1)
    frames = [
        make_fdau_frame(
            seq=0,
            ts_monotonic=1.00,
            major_frame=0,
            tick=0,
            values={"IAS": 101.0, "TAS": 201.0, "LandingGear": 0.0},
        ),
        make_fdau_frame(
            seq=1,
            ts_monotonic=1.25,
            major_frame=0,
            tick=1,
            values={"IAS": 102.0, "TAS": 202.0, "LandingGear": 1.0},
        ),
        make_fdau_frame(
            seq=2,
            ts_monotonic=1.50,
            major_frame=0,
            tick=2,
            values={},
        ),
    ]

    try:
        assert fdau.flt.layout[0].name == "IAS"
        service.start()
        for frame in frames:
            service.on_frame(frame)
        service.stop(join=True, timeout=2)

        stats = service.stats()
        assert stats["frames_in"] == len(frames)
        assert stats["rows_out"] == 2
        assert stats["points_out"] == 6
        assert point_rows(storage.query_range(1_000_000_000, 1_600_000_000)) == [
            {"timestamp_ns": 1_000_000_000, "parameter_id": 1, "value": 101.0},
            {"timestamp_ns": 1_000_000_000, "parameter_id": 2, "value": 201.0},
            {"timestamp_ns": 1_000_000_000, "parameter_id": 3, "value": 0.0},
            {"timestamp_ns": 1_250_000_000, "parameter_id": 1, "value": 102.0},
            {"timestamp_ns": 1_250_000_000, "parameter_id": 2, "value": 202.0},
            {"timestamp_ns": 1_250_000_000, "parameter_id": 3, "value": 1.0},
        ]
    finally:
        service.stop(join=True, timeout=2)
        storage.close()


def test_mvp_ingest_flushes_to_storage_by_batch_max_points(tmp_path):
    """Point-count batching flushes into the real StorageCore backend."""
    storage = StorageCore(config=make_storage_config(tmp_path, flush_max_rows=100))
    service = IngestService(
        settings=make_ingest_settings(batch_max_points=3),
        storage=storage,
    )
    frames = [
        make_fdau_frame(
            seq=0,
            ts_monotonic=2.00,
            major_frame=0,
            tick=0,
            values={"IAS": 10.0, "TAS": 100.0},
        ),
        make_fdau_frame(
            seq=1,
            ts_monotonic=2.25,
            major_frame=0,
            tick=1,
            values={"IAS": 20.0, "TAS": 200.0},
        ),
    ]

    try:
        service.start()
        for frame in frames:
            service.on_frame(frame)

        wait_until(lambda: service.stats()["points_out"] == 4)

        assert service.stats()["rows_out"] == 2
        assert point_rows(storage.query_range(2_000_000_000, 2_300_000_000)) == [
            {"timestamp_ns": 2_000_000_000, "parameter_id": 1, "value": 10.0},
            {"timestamp_ns": 2_000_000_000, "parameter_id": 2, "value": 100.0},
            {"timestamp_ns": 2_250_000_000, "parameter_id": 1, "value": 20.0},
            {"timestamp_ns": 2_250_000_000, "parameter_id": 2, "value": 200.0},
        ]
    finally:
        service.stop(join=True, timeout=2)
        storage.close()


def test_mvp_query_range_filters_single_parameter(tmp_path):
    """Single-parameter query uses a half-open time interval."""
    storage = StorageCore(config=make_storage_config(tmp_path))
    storage.append_rows(
        [
            (100, [(1, 10.0), (2, 100.0)]),
            (110, [(1, 20.0), (2, 200.0)]),
            (120, [(3, 300.0)]),
        ]
    )

    assert point_rows(storage.query_range(100, 120, {1})) == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 10.0},
        {"timestamp_ns": 110, "parameter_id": 1, "value": 20.0},
    ]
    storage.close()


def test_mvp_query_range_filters_multiple_parameters_across_memtable_and_sstable(
    tmp_path,
):
    """Multi-parameter query merges SSTable and memtable reads."""
    storage = StorageCore(config=make_storage_config(tmp_path, flush_max_rows=100))
    storage.append_rows(
        [
            (100, [(1, 10.0), (2, 100.0)]),
            (110, [(1, 20.0), (3, 300.0)]),
        ]
    )
    storage.flush()
    storage.append_rows([(115, [(2, 150.0), (3, 350.0)])])

    assert point_rows(storage.query_range(100, 120, {2, 3})) == [
        {"timestamp_ns": 100, "parameter_id": 2, "value": 100.0},
        {"timestamp_ns": 110, "parameter_id": 3, "value": 300.0},
        {"timestamp_ns": 115, "parameter_id": 2, "value": 150.0},
        {"timestamp_ns": 115, "parameter_id": 3, "value": 350.0},
    ]
    storage.close()


def test_mvp_storage_uses_sstable_files_and_compaction_preserves_reads(tmp_path):
    """SSTable flush and compaction keep data queryable through the public API."""
    write_config = make_storage_config(tmp_path, flush_max_rows=100)
    storage = StorageCore(config=write_config)
    storage.append_rows([(100, [(1, 10.0)]), (110, [(2, 20.0)])])
    storage.flush()
    storage.append_rows([(120, [(1, 30.0)]), (130, [(2, 40.0)])])
    storage.flush()
    storage.close()

    compact_config = make_storage_config(tmp_path, compaction_min_tables=2)
    storage = StorageCore(config=compact_config)
    try:
        before = point_rows(storage.query_range(0, 200, None))
        old_files = sorted(path.name for path in compact_config.sst_dir.glob("*.sst"))

        assert len(old_files) == 2
        assert storage.compact() is True
        assert point_rows(storage.query_range(0, 200, None)) == before

        compacted_files = sorted(
            path.name for path in compact_config.sst_dir.glob("*.sst")
        )
        assert len(compacted_files) == 1
        assert compacted_files[0] not in old_files
    finally:
        storage.close()


def test_mvp_query_range_can_run_while_appends_continue(tmp_path):
    """Range reads and appends can overlap without stopping the writer."""
    storage = StorageCore(
        config=make_storage_config(
            tmp_path,
            flush_max_rows=1_000,
            compaction_min_tables=10,
        )
    )
    failures: list[str] = []
    reader_started = threading.Event()
    writer_done = threading.Event()

    def reader() -> None:
        try:
            reader_started.set()
            while not writer_done.is_set():
                storage.query_range(100, 200, {1})
        except Exception as exc:  # pragma: no cover - failure path assertion
            failures.append(f"reader:{exc}")

    def writer() -> None:
        try:
            assert reader_started.wait(timeout=1.0)
            for timestamp_ns in range(100, 120):
                storage.append_rows([(timestamp_ns, [(1, float(timestamp_ns))])])
        except Exception as exc:  # pragma: no cover - failure path assertion
            failures.append(f"writer:{exc}")
        finally:
            writer_done.set()

    reader_thread = threading.Thread(target=reader, name="mvp-query-reader")
    writer_thread = threading.Thread(target=writer, name="mvp-append-writer")
    reader_thread.start()
    writer_thread.start()
    writer_thread.join(timeout=1.0)
    writer_done.set()
    reader_thread.join(timeout=1.0)

    try:
        assert not writer_thread.is_alive()
        assert not reader_thread.is_alive()
        assert failures == []
        assert point_rows(storage.query_range(100, 120, {1})) == [
            {
                "timestamp_ns": timestamp_ns,
                "parameter_id": 1,
                "value": float(timestamp_ns),
            }
            for timestamp_ns in range(100, 120)
        ]
    finally:
        storage.close()


def test_mvp_aggregate_range_single_parameter(tmp_path):
    """Aggregate one requested parameter over a half-open interval."""
    storage = StorageCore(config=make_storage_config(tmp_path))
    storage.append_rows(
        [
            (100, [(1, 10.0), (2, 100.0)]),
            (110, [(1, 20.0), (2, 200.0)]),
            (120, [(1, 999.0)]),
        ]
    )

    assert aggregate_rows(storage.aggregate_range(100, 120, {1})) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 1,
            "count": 2,
            "min": 10.0,
            "max": 20.0,
            "avg": 15.0,
        }
    ]
    storage.close()


def test_mvp_aggregate_range_multiple_parameters(tmp_path):
    """Aggregate requested parameters across SSTable and memtable sources."""
    storage = StorageCore(config=make_storage_config(tmp_path, flush_max_rows=100))
    storage.append_rows(
        [
            (100, [(1, 10.0), (2, 100.0), (3, 1000.0)]),
            (110, [(1, 20.0), (2, 200.0), (3, 2000.0)]),
        ]
    )
    storage.flush()
    storage.append_rows([(120, [(1, 30.0), (2, 300.0), (3, 3000.0)])])

    assert aggregate_rows(storage.aggregate_range(100, 130, {1, 2})) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 130,
            "parameter_id": 1,
            "count": 3,
            "min": 10.0,
            "max": 30.0,
            "avg": 20.0,
        },
        {
            "start_ts_ns": 100,
            "end_ts_ns": 130,
            "parameter_id": 2,
            "count": 3,
            "min": 100.0,
            "max": 300.0,
            "avg": 200.0,
        },
    ]
    storage.close()


def test_mvp_points_csv_export_from_query_result(tmp_path):
    """Query points export to canonical CSV rows and headers."""
    storage = StorageCore(config=make_storage_config(tmp_path))
    storage.append_rows([(100, [(1, 10.0), (2, 20.5)]), (110, [(1, 30.0)])])
    points = storage.query_range(100, 120, {1, 2})
    target = StringIO()

    assert points_to_rows(points) == [
        {"timestamp_ns": 100, "parameter_id": 1, "value": 10.0},
        {"timestamp_ns": 100, "parameter_id": 2, "value": 20.5},
        {"timestamp_ns": 110, "parameter_id": 1, "value": 30.0},
    ]

    write_points_csv(points, target)
    fieldnames, rows = read_csv(target.getvalue())

    assert fieldnames == ["timestamp_ns", "parameter_id", "value"]
    assert rows == [
        {"timestamp_ns": "100", "parameter_id": "1", "value": "10.0"},
        {"timestamp_ns": "100", "parameter_id": "2", "value": "20.5"},
        {"timestamp_ns": "110", "parameter_id": "1", "value": "30.0"},
    ]
    storage.close()


def test_mvp_aggregates_csv_export_from_aggregate_result(tmp_path):
    """Aggregate results export to canonical CSV rows and headers."""
    storage = StorageCore(config=make_storage_config(tmp_path))
    storage.append_rows([(100, [(1, 10.0), (2, 100.0)]), (110, [(1, 20.0)])])
    results = storage.aggregate_range(100, 120, {1})
    target = StringIO()

    assert aggregates_to_rows(results) == [
        {
            "start_ts_ns": 100,
            "end_ts_ns": 120,
            "parameter_id": 1,
            "count": 2,
            "min": 10.0,
            "max": 20.0,
            "avg": 15.0,
        }
    ]

    write_aggregates_csv(results, target)
    fieldnames, rows = read_csv(target.getvalue())

    assert fieldnames == [
        "start_ts_ns",
        "end_ts_ns",
        "parameter_id",
        "count",
        "min",
        "max",
        "avg",
    ]
    assert rows == [
        {
            "start_ts_ns": "100",
            "end_ts_ns": "120",
            "parameter_id": "1",
            "count": "2",
            "min": "10.0",
            "max": "20.0",
            "avg": "15.0",
        }
    ]
    storage.close()


def test_mvp_recovery_after_graceful_close_preserves_memtable_data(tmp_path):
    """Graceful close flushes memtable data so a new StorageCore can recover it."""
    config = make_storage_config(tmp_path, flush_max_rows=100)
    storage = StorageCore(config=config)
    storage.append_rows([(100, [(1, 10.0)]), (110, [(2, 20.0)])])

    storage.close()

    assert config.manifest_path.exists()
    assert list(config.sst_dir.glob("*.sst"))

    recovered = StorageCore(config=config)
    try:
        assert point_rows(recovered.query_range(0, 200, None)) == [
            {"timestamp_ns": 100, "parameter_id": 1, "value": 10.0},
            {"timestamp_ns": 110, "parameter_id": 2, "value": 20.0},
        ]
    finally:
        recovered.close()
