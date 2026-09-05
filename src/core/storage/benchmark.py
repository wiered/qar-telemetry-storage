"""Benchmark harness for storage runtime and SSTable formats."""

from __future__ import annotations

from dataclasses import fields
from datetime import datetime, timezone
import json
import platform
import random
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from typing import Any

from ..ingest import FramePayload, IngestFrame, IngestService
from .config import SSTableFormat, StorageRuntimeConfig
from .core import StorageCore
from .models import Row, StorageStats


def generate_dataset(
    *,
    seed: int,
    rows_count: int,
    parameter_pool: int,
    points_per_row: int,
    base_timestamp_ns: int = 1_700_000_000_000_000_000,
    step_ns: int = 125_000_000,
) -> list[Row]:
    """Generate deterministic synthetic rows for benchmark scenarios.

    Args:
        seed: Random seed for reproducible generation.
        rows_count: Number of rows to generate.
        parameter_pool: Maximum distinct parameter IDs available.
        points_per_row: Target average number of points in each row.
        base_timestamp_ns: Initial timestamp for the first row.
        step_ns: Baseline timestamp increment between rows.

    Returns:
        list[Row]: Generated dataset ordered by increasing timestamps.

    """
    rng = random.Random(seed)
    rows: list[Row] = []

    timestamp_ns = base_timestamp_ns
    for _ in range(rows_count):
        active_points = rng.randint(max(1, points_per_row - 2), points_per_row + 2)
        parameter_ids = sorted(
            rng.sample(
                range(1, parameter_pool + 1), k=min(parameter_pool, active_points)
            )
        )
        values = [
            (
                parameter_id,
                round(
                    (parameter_id * 0.125)
                    + (rng.random() * 3.0)
                    + ((timestamp_ns % 1_000_000_000) / 1_000_000_000),
                    6,
                ),
            )
            for parameter_id in parameter_ids
        ]
        rows.append((timestamp_ns, values))
        timestamp_ns += step_ns + rng.randint(0, step_ns // 8)
    return rows


def run_benchmark_harness(
    *,
    seed: int,
    rows_count: int,
    parameter_pool: int,
    points_per_row: int,
    block_max_points: int,
    sweep_candidates: list[int],
    output_json: Path | None = None,
) -> dict[str, Any]:
    """Execute benchmark suites and optional block-size sweep.

    Args:
        seed: Random seed for dataset generation.
        rows_count: Number of rows in generated dataset.
        parameter_pool: Maximum distinct parameter IDs in generated data.
        points_per_row: Target average points per row.
        block_max_points: SSTable block size for primary benchmark runs.
        sweep_candidates: Block-size values for sweep recommendation.
        output_json: Optional path where JSON report is written.

    Returns:
        dict[str, Any]: Full benchmark report payload.

    """
    dataset = generate_dataset(
        seed=seed,
        rows_count=rows_count,
        parameter_pool=parameter_pool,
        points_per_row=points_per_row,
    )
    total_points = sum(len(values) for _, values in dataset)

    runs = [
        _run_format_suite(
            format_name="baseline",
            sstable_format="v1_raw",
            dataset=dataset,
            block_max_points=block_max_points,
        ),
        _run_format_suite(
            format_name="optimized",
            sstable_format="v2_timeseries",
            dataset=dataset,
            block_max_points=block_max_points,
        ),
    ]
    sweep = _run_block_sweep(dataset=dataset, candidates=sweep_candidates)

    report = {
        "seed": seed,
        "environment": {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "parameters": {
            "seed": seed,
            "rows_count": rows_count,
            "parameter_pool": parameter_pool,
            "points_per_row": points_per_row,
            "block_max_points": block_max_points,
            "sweep_candidates": sweep_candidates,
        },
        "dataset": {
            "rows": rows_count,
            "points": total_points,
            "parameter_pool": parameter_pool,
            "points_per_row_target": points_per_row,
        },
        "block_max_points": block_max_points,
        "runs": runs,
        "sweep": sweep,
    }

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    return report


def run_scale_benchmark_harness(
    *,
    seed: int,
    rows_count: int,
    parameter_pools: list[int],
    block_max_points: int,
    output_json: Path | None = None,
) -> dict[str, Any]:
    """Execute write-focused scale benchmark for larger FDAU parameter pools.

    Each candidate uses ``points_per_row == parameter_pool`` to emulate stress
    frames where all parameters arrive in every FDAU tick.
    """
    runs: list[dict[str, Any]] = []
    for parameter_pool in parameter_pools:
        dataset = generate_dataset(
            seed=seed,
            rows_count=rows_count,
            parameter_pool=parameter_pool,
            points_per_row=parameter_pool,
        )
        total_points = sum(len(values) for _, values in dataset)
        scenarios = _run_scale_suite(
            dataset=dataset,
            block_max_points=block_max_points,
        )
        runs.append(
            {
                "parameter_pool": parameter_pool,
                "points_per_row_target": parameter_pool,
                "rows": rows_count,
                "points": total_points,
                "scenarios": scenarios,
            }
        )

    report = {
        "seed": seed,
        "environment": {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "parameters": {
            "seed": seed,
            "rows_count": rows_count,
            "parameter_pools": parameter_pools,
            "block_max_points": block_max_points,
            "sstable_format": "v2_timeseries",
        },
        "runs": runs,
    }

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    return report


def run_capture_benchmark_harness(
    *,
    seed: int,
    frames_count: int,
    parameter_pool: int,
    points_per_frame: int,
    queue_max_frames: int,
    batch_max_rows: int,
    batch_max_points: int,
    batch_max_ms: int | float,
    block_max_points: int,
    output_json: Path | None = None,
) -> dict[str, Any]:
    """Measure FDAU-frame -> ingest queue -> StorageCore capture metrics.

    This benchmark covers four system-level metrics: end-to-end capture rate,
    data loss rate, write latency p95/p99, and write throughput. It uses
    synthetic but valid frames and drives ``IngestService`` as fast as possible.
    """
    frames = _generate_ingest_frames(
        seed=seed,
        frames_count=frames_count,
        parameter_pool=parameter_pool,
        points_per_frame=points_per_frame,
    )
    generated_points = sum(len(frame["values"]) for frame in frames)
    generated_frames = len(frames)

    with tempfile.TemporaryDirectory(prefix="qar-bench-capture-") as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format="v2_timeseries",
            block_max_points=block_max_points,
            flush_max_rows=batch_max_rows,
            compaction_min_tables=32,
        )
        storage = _LatencyTrackingStorage(StorageCore(config=config))
        ingest = IngestService(
            settings=_make_ingest_benchmark_settings(
                queue_max_frames=queue_max_frames,
                batch_max_rows=batch_max_rows,
                batch_max_points=batch_max_points,
                batch_max_ms=batch_max_ms,
            ),
            storage=storage,
        )

        started = time.perf_counter()
        ingest.start()
        for frame in frames:
            ingest.on_frame(frame)
        ingest.stop(join=True, timeout=max(10.0, frames_count / 1000.0))
        storage.flush()
        elapsed = max(1e-9, time.perf_counter() - started)
        ingest_stats = ingest.stats()
        storage_stats = storage.stats_snapshot()
        storage.close()

    stored_points = int(ingest_stats["points_out"])
    stored_frames = int(ingest_stats["frames_in"])
    lost_points = max(0, generated_points - stored_points)
    loss_rate = lost_points / max(1, generated_points)
    append_latency_ms = storage.append_latency_ms
    write_elapsed = max(1e-9, storage.append_elapsed_s)

    report = {
        "seed": seed,
        "environment": {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "parameters": {
            "seed": seed,
            "frames_count": frames_count,
            "parameter_pool": parameter_pool,
            "points_per_frame": points_per_frame,
            "queue_max_frames": queue_max_frames,
            "batch_max_rows": batch_max_rows,
            "batch_max_points": batch_max_points,
            "batch_max_ms": batch_max_ms,
            "block_max_points": block_max_points,
            "sstable_format": "v2_timeseries",
        },
        "metrics": {
            "generated_frames": generated_frames,
            "generated_points": generated_points,
            "frames_in": stored_frames,
            "rows_out": int(ingest_stats["rows_out"]),
            "points_out": stored_points,
            "dropped_frames": int(ingest_stats["dropped_frames"]),
            "seq_gaps": int(ingest_stats["seq_gaps"]),
            "lost_points": lost_points,
            "data_loss_rate": loss_rate,
            "data_loss_percent": loss_rate * 100.0,
            "end_to_end_capture_points_per_sec": stored_points / elapsed,
            "end_to_end_capture_frames_per_sec": stored_frames / elapsed,
            "write_throughput_points_per_sec": stored_points / write_elapsed,
            "write_throughput_rows_per_sec": max(0, int(ingest_stats["rows_out"]))
            / write_elapsed,
            "write_latency_p50_ms": _percentile(append_latency_ms, 50),
            "write_latency_p95_ms": _percentile(append_latency_ms, 95),
            "write_latency_p99_ms": _percentile(append_latency_ms, 99),
            "append_calls": storage.append_calls,
            "elapsed_ms": elapsed * 1000.0,
            "storage_bytes_written": storage_stats.bytes_written,
            "storage_flush_count": storage_stats.flush_count,
            "storage_compaction_count": storage_stats.compaction_count,
        },
    }

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    return report


def print_human_summary(report: dict[str, Any]) -> None:
    """Print compact human-readable summary from benchmark report.

    Args:
        report: Report payload produced by `run_benchmark_harness`.

    """
    print(
        f"Dataset: rows={report['dataset']['rows']} points={report['dataset']['points']} "
        f"seed={report['seed']} block_max_points={report['block_max_points']}"
    )
    for run in report["runs"]:
        print(f"\n[{run['format_name']}] format={run['sstable_format']}")
        for scenario_name, metrics in run["scenarios"].items():
            print(
                f"  {scenario_name}: rows/s={metrics.get('rows_per_sec', 0):.1f} "
                f"points/s={metrics.get('points_per_sec', 0):.1f} "
                f"bytes/point={metrics.get('bytes_per_point', 0):.3f} "
                f"p50={metrics.get('latency_p50_ms', 0):.3f}ms "
                f"p95={metrics.get('latency_p95_ms', 0):.3f}ms "
                f"files_opened={metrics.get('files_opened', 0)} "
                f"blocks_scanned={metrics.get('blocks_scanned', 0)} "
                f"quarantined={metrics.get('quarantined_files', 0)}"
            )
    sweep = report["sweep"]
    print(
        f"\nSweep recommendation: block_max_points={sweep['selected_block_max_points']} "
        f"(best ingest rows/s={sweep['best_ingest_rows_per_sec']:.1f})"
    )


def print_scale_human_summary(report: dict[str, Any]) -> None:
    """Print compact scale benchmark summary from report payload."""
    print(
        f"Scale dataset: rows={report['parameters']['rows_count']} "
        f"pools={','.join(str(pool) for pool in report['parameters']['parameter_pools'])} "
        f"block_max_points={report['parameters']['block_max_points']}"
    )
    for run in report["runs"]:
        ingest = run["scenarios"]["ingest_only"]
        query = run["scenarios"]["wide_query"]
        recovery = run["scenarios"]["cold_recovery_startup"]
        print(
            f"  pool={run['parameter_pool']}: points={run['points']} "
            f"rows/s={ingest['rows_per_sec']:.1f} "
            f"points/s={ingest['points_per_sec']:.1f} "
            f"bytes/point={ingest['bytes_per_point']:.3f} "
            f"wide_p95={query['latency_p95_ms']:.3f}ms "
            f"recovery={recovery['recovery_duration_ms']:.3f}ms"
        )


def print_capture_human_summary(report: dict[str, Any]) -> None:
    """Print compact capture benchmark summary."""
    metrics = report["metrics"]
    params = report["parameters"]
    print(
        f"Capture dataset: frames={params['frames_count']} "
        f"points/frame={params['points_per_frame']} "
        f"pool={params['parameter_pool']} block_max_points={params['block_max_points']}"
    )
    print(
        "  end_to_end_capture="
        f"{metrics['end_to_end_capture_points_per_sec']:.1f} points/s "
        f"({metrics['end_to_end_capture_frames_per_sec']:.1f} frames/s)"
    )
    print(
        "  data_loss="
        f"{metrics['data_loss_percent']:.6f}% "
        f"lost_points={metrics['lost_points']} dropped_frames={metrics['dropped_frames']}"
    )
    print(
        "  write_latency="
        f"p50={metrics['write_latency_p50_ms']:.3f}ms "
        f"p95={metrics['write_latency_p95_ms']:.3f}ms "
        f"p99={metrics['write_latency_p99_ms']:.3f}ms"
    )
    print(
        "  write_throughput="
        f"{metrics['write_throughput_points_per_sec']:.1f} points/s "
        f"{metrics['write_throughput_rows_per_sec']:.1f} rows/s "
        f"append_calls={metrics['append_calls']}"
    )


def _run_format_suite(
    *,
    format_name: str,
    sstable_format: SSTableFormat,
    dataset: list[Row],
    block_max_points: int,
) -> dict[str, Any]:
    """Run all benchmark scenarios for one SSTable format.

    Args:
        format_name: Human-friendly label of tested format.
        sstable_format: Storage format under test.
        dataset: Input dataset for all scenarios.
        block_max_points: Block-size limit for SSTables.

    Returns:
        dict[str, Any]: Scenario metrics grouped by scenario name.

    """
    return {
        "format_name": format_name,
        "sstable_format": sstable_format,
        "scenarios": {
            "ingest_only": _scenario_ingest_only(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
            ),
            "narrow_query": _scenario_query(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
                wide=False,
            ),
            "wide_query": _scenario_query(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
                wide=True,
            ),
            "mixed_read_write": _scenario_mixed_read_write(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
            ),
            "frequent_flush_compact": _scenario_frequent_flush_compact(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
            ),
            "cold_recovery_startup": _scenario_cold_recovery_startup(
                dataset=dataset,
                sstable_format=sstable_format,
                block_max_points=block_max_points,
            ),
        },
    }


def _run_scale_suite(
    *,
    dataset: list[Row],
    block_max_points: int,
) -> dict[str, Any]:
    """Run v2 scale scenarios for one parameter pool."""
    return {
        "ingest_only": _scenario_ingest_only(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=block_max_points,
        ),
        "wide_query": _scenario_query(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=block_max_points,
            wide=True,
        ),
        "cold_recovery_startup": _scenario_cold_recovery_startup(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=block_max_points,
        ),
    }


def _scenario_ingest_only(
    *,
    dataset: list[Row],
    sstable_format: SSTableFormat,
    block_max_points: int,
) -> dict[str, Any]:
    """Measure pure ingest throughput and write amplification metrics.

    Args:
        dataset: Rows appended to storage.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.

    Returns:
        dict[str, Any]: Throughput and write-related metrics.

    """
    with tempfile.TemporaryDirectory(
        prefix=f"qar-bench-{sstable_format}-ingest-"
    ) as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format=sstable_format,
            block_max_points=block_max_points,
            flush_max_rows=512,
            compaction_min_tables=32,
        )
        storage = StorageCore(config=config)
        before = storage.stats_snapshot()
        started = time.perf_counter()
        _append_dataset(storage, dataset, batch_rows=128)
        storage.flush()
        elapsed = max(1e-9, time.perf_counter() - started)
        after = storage.stats_snapshot()
        storage.close()
        delta = _stats_diff(before, after)
        total_points = sum(len(values) for _, values in dataset)
        return {
            "rows_per_sec": len(dataset) / elapsed,
            "points_per_sec": total_points / elapsed,
            "bytes_written": delta["bytes_written"],
            "bytes_per_point": delta["bytes_written"] / max(1, total_points),
            "flush_count": delta["flush_count"],
        }


def _scenario_query(
    *,
    dataset: list[Row],
    sstable_format: SSTableFormat,
    block_max_points: int,
    wide: bool,
) -> dict[str, Any]:
    """Measure query latency and pruning efficiency for recovered storage.

    Args:
        dataset: Rows used to initialize test storage.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.
        wide: Whether to build wide (True) or narrow (False) queries.

    Returns:
        dict[str, Any]: Query throughput, latency, and scan counters.

    """
    with tempfile.TemporaryDirectory(
        prefix=f"qar-bench-{sstable_format}-query-"
    ) as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format=sstable_format,
            block_max_points=block_max_points,
            flush_max_rows=256,
            compaction_min_tables=16,
        )
        storage = StorageCore(config=config)
        _append_dataset(storage, dataset, batch_rows=64)
        storage.close()

        recovered = StorageCore(config=config)
        before = recovered.stats_snapshot()
        latencies_ms: list[float] = []
        total_rows = 0
        total_points = 0
        query_specs = _build_query_specs(dataset, wide=wide)
        for start_ts_ns, end_ts_ns, parameter_ids in query_specs:
            started = time.perf_counter()
            points = recovered.query_range(start_ts_ns, end_ts_ns, parameter_ids)
            latencies_ms.append((time.perf_counter() - started) * 1000.0)
            total_rows += 1
            total_points += len(points)
        after = recovered.stats_snapshot()
        recovered.close()
        delta = _stats_diff(before, after)
        return {
            "rows_per_sec": total_rows / max(1e-9, sum(latencies_ms) / 1000.0),
            "points_per_sec": total_points / max(1e-9, sum(latencies_ms) / 1000.0),
            "latency_p50_ms": _percentile(latencies_ms, 50),
            "latency_p95_ms": _percentile(latencies_ms, 95),
            "files_considered": delta["files_considered"],
            "files_pruned": delta["files_pruned"],
            "files_opened": delta["files_opened"],
            "blocks_considered": delta["blocks_considered"],
            "blocks_pruned": delta["blocks_pruned"],
            "blocks_scanned": delta["blocks_scanned"],
            "points_decoded": delta["points_decoded"],
            "points_returned": delta["points_returned"],
        }


def _scenario_mixed_read_write(
    *,
    dataset: list[Row],
    sstable_format: SSTableFormat,
    block_max_points: int,
) -> dict[str, Any]:
    """Measure concurrent read/write behavior with lightweight contention.

    Args:
        dataset: Rows written while reads are issued.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.

    Returns:
        dict[str, Any]: Throughput, query latencies, and failure list.

    """
    with tempfile.TemporaryDirectory(
        prefix=f"qar-bench-{sstable_format}-mixed-"
    ) as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format=sstable_format,
            block_max_points=block_max_points,
            flush_max_rows=96,
            compaction_min_tables=8,
        )
        storage = StorageCore(config=config)
        latencies_ms: list[float] = []
        failures: list[str] = []
        query_specs = _build_query_specs(dataset, wide=False)

        def writer() -> None:
            try:
                _append_dataset(storage, dataset, batch_rows=24, sleep_s=0.001)
            except Exception as exc:
                failures.append(f"writer:{exc}")

        def reader() -> None:
            try:
                for start_ts_ns, end_ts_ns, parameter_ids in query_specs:
                    started = time.perf_counter()
                    storage.query_range(start_ts_ns, end_ts_ns, parameter_ids)
                    latencies_ms.append((time.perf_counter() - started) * 1000.0)
                    time.sleep(0.001)
            except Exception as exc:
                failures.append(f"reader:{exc}")

        before = storage.stats_snapshot()
        started = time.perf_counter()
        writer_thread = threading.Thread(target=writer, name="bench-writer")
        reader_thread = threading.Thread(target=reader, name="bench-reader")
        writer_thread.start()
        reader_thread.start()
        writer_thread.join()
        reader_thread.join()
        storage.close()
        elapsed = max(1e-9, time.perf_counter() - started)
        after = storage.stats_snapshot()
        delta = _stats_diff(before, after)
        total_points = sum(len(values) for _, values in dataset)
        return {
            "rows_per_sec": len(dataset) / elapsed,
            "points_per_sec": total_points / elapsed,
            "latency_p50_ms": _percentile(latencies_ms, 50),
            "latency_p95_ms": _percentile(latencies_ms, 95),
            "files_opened": delta["files_opened"],
            "blocks_scanned": delta["blocks_scanned"],
            "failures": failures,
        }


def _scenario_frequent_flush_compact(
    *,
    dataset: list[Row],
    sstable_format: SSTableFormat,
    block_max_points: int,
) -> dict[str, Any]:
    """Measure performance under aggressive flush and compaction cadence.

    Args:
        dataset: Rows to append before flush and compaction.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.

    Returns:
        dict[str, Any]: Throughput plus compaction rewrite statistics.

    """
    with tempfile.TemporaryDirectory(
        prefix=f"qar-bench-{sstable_format}-compact-"
    ) as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format=sstable_format,
            block_max_points=block_max_points,
            flush_max_rows=32,
            compaction_min_tables=4,
        )
        storage = StorageCore(config=config)
        before = storage.stats_snapshot()
        started = time.perf_counter()
        _append_dataset(storage, dataset, batch_rows=16)
        storage.flush()
        storage.compact()
        elapsed = max(1e-9, time.perf_counter() - started)
        after = storage.stats_snapshot()
        storage.close()
        delta = _stats_diff(before, after)
        total_points = sum(len(values) for _, values in dataset)
        return {
            "rows_per_sec": len(dataset) / elapsed,
            "points_per_sec": total_points / elapsed,
            "bytes_written": delta["bytes_written"],
            "bytes_per_point": delta["bytes_written"] / max(1, total_points),
            "compaction_count": delta["compaction_count"],
            "compaction_duration_ms": delta["compaction_duration_ns"] / 1_000_000,
            "compaction_rewrite_points": delta["compaction_rewrite_points"],
            "compaction_rewrite_bytes": delta["compaction_rewrite_bytes"],
        }


def _scenario_cold_recovery_startup(
    *,
    dataset: list[Row],
    sstable_format: SSTableFormat,
    block_max_points: int,
) -> dict[str, Any]:
    """Measure cold-start recovery time after process restart.

    Args:
        dataset: Rows used to create on-disk test state.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.

    Returns:
        dict[str, Any]: Recovery duration and startup health counters.

    """
    with tempfile.TemporaryDirectory(
        prefix=f"qar-bench-{sstable_format}-recovery-"
    ) as tmp_dir:
        config = _make_config(
            Path(tmp_dir),
            sstable_format=sstable_format,
            block_max_points=block_max_points,
            flush_max_rows=256,
            compaction_min_tables=16,
        )
        storage = StorageCore(config=config)
        _append_dataset(storage, dataset, batch_rows=64)
        storage.close()

        started = time.perf_counter()
        recovered = StorageCore(config=config)
        elapsed = max(1e-9, time.perf_counter() - started)
        stats = recovered.stats_snapshot()
        recovered.close()
        return {
            "rows_per_sec": len(dataset) / elapsed,
            "points_per_sec": sum(len(values) for _, values in dataset) / elapsed,
            "recovery_duration_ms": stats.recovery_duration_ns / 1_000_000,
            "quarantined_files": stats.quarantined_files,
            "manifest_rebuild_count": stats.manifest_rebuild_count,
        }


def _run_block_sweep(
    *,
    dataset: list[Row],
    candidates: list[int],
) -> dict[str, Any]:
    """Sweep block sizes and choose a balanced recommendation.

    Args:
        dataset: Dataset used across all sweep candidates.
        candidates: Block-size candidates to evaluate.

    Returns:
        dict[str, Any]: Candidate metrics and selected block-size value.

    """
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        ingest_metrics = _scenario_ingest_only(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=candidate,
        )
        narrow_metrics = _scenario_query(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=candidate,
            wide=False,
        )
        compact_metrics = _scenario_frequent_flush_compact(
            dataset=dataset,
            sstable_format="v2_timeseries",
            block_max_points=candidate,
        )
        results.append(
            {
                "block_max_points": candidate,
                "ingest_rows_per_sec": ingest_metrics["rows_per_sec"],
                "narrow_latency_p95_ms": narrow_metrics["latency_p95_ms"],
                "bytes_per_point": compact_metrics["bytes_per_point"],
                "compaction_duration_ms": compact_metrics["compaction_duration_ms"],
                "compaction_rewrite_bytes": compact_metrics["compaction_rewrite_bytes"],
            }
        )

    best_ingest = max(result["ingest_rows_per_sec"] for result in results)
    eligible = [
        result
        for result in results
        if result["ingest_rows_per_sec"] >= best_ingest * 0.9
    ]
    selected = min(
        eligible,
        key=lambda result: (
            result["narrow_latency_p95_ms"],
            result["bytes_per_point"],
            result["compaction_duration_ms"],
            result["compaction_rewrite_bytes"],
        ),
    )
    return {
        "candidates": results,
        "best_ingest_rows_per_sec": best_ingest,
        "selected_block_max_points": selected["block_max_points"],
    }


def _make_config(
    root_dir: Path,
    *,
    sstable_format: SSTableFormat,
    block_max_points: int,
    flush_max_rows: int,
    compaction_min_tables: int,
) -> StorageRuntimeConfig:
    """Build storage runtime configuration for a benchmark scenario.

    Args:
        root_dir: Temporary root directory for scenario files.
        sstable_format: Storage format under test.
        block_max_points: Block-size limit for SSTables.
        flush_max_rows: Row-count threshold for memtable flush.
        compaction_min_tables: Minimum table count to trigger compaction.

    Returns:
        StorageRuntimeConfig: Scenario-specific runtime configuration.

    """
    return StorageRuntimeConfig(
        data_dir=root_dir / "storage",
        flush_max_rows=flush_max_rows,
        flush_max_points=200_000,
        flush_max_bytes=64 * 1024 * 1024,
        sstable_block_max_points=block_max_points,
        compaction_min_tables=compaction_min_tables,
        sstable_format=sstable_format,
        cleanup_temp_on_startup=True,
        quarantine_dir_name="quarantine",
    )


def _append_dataset(
    storage: StorageCore,
    dataset: list[Row],
    *,
    batch_rows: int,
    sleep_s: float = 0.0,
) -> None:
    """Append dataset to storage in batches with optional pacing delay.

    Args:
        storage: Storage instance receiving rows.
        dataset: Rows to append.
        batch_rows: Batch size used per append call.
        sleep_s: Optional sleep duration between batches.

    """
    for offset in range(0, len(dataset), batch_rows):
        storage.append_rows(dataset[offset : offset + batch_rows])
        if sleep_s > 0:
            time.sleep(sleep_s)


class _LatencyTrackingStorage:
    """StorageCore wrapper that records ``append_rows`` latency samples."""

    def __init__(self, storage: StorageCore) -> None:
        self._storage = storage
        self.append_latency_ms: list[float] = []
        self.append_calls = 0
        self.append_elapsed_s = 0.0

    def append_rows(self, rows: list[Row]) -> None:
        started = time.perf_counter()
        self._storage.append_rows(rows)
        elapsed = time.perf_counter() - started
        self.append_calls += 1
        self.append_elapsed_s += elapsed
        self.append_latency_ms.append(elapsed * 1000.0)

    def flush(self) -> None:
        self._storage.flush()

    def close(self) -> None:
        self._storage.close()

    def stats_snapshot(self) -> StorageStats:
        return self._storage.stats_snapshot()


def _make_ingest_benchmark_settings(
    *,
    queue_max_frames: int,
    batch_max_rows: int,
    batch_max_points: int,
    batch_max_ms: int | float,
) -> SimpleNamespace:
    """Build the minimal settings object consumed by ``IngestService``."""
    return SimpleNamespace(
        ingest=SimpleNamespace(
            queue_max_frames=queue_max_frames,
            batch_max_rows=batch_max_rows,
            batch_max_points=batch_max_points,
            batch_max_ms=batch_max_ms,
            overflow_policy="drop_newest",
            warn_every_dropped=10_000,
            idle_sleep_ms=1,
        )
    )


def _generate_ingest_frames(
    *,
    seed: int,
    frames_count: int,
    parameter_pool: int,
    points_per_frame: int,
) -> list[IngestFrame]:
    """Generate deterministic frames shaped like FDAU output."""
    rng = random.Random(seed)
    frames: list[IngestFrame] = []
    base_ts = 1_700_000_000.0
    for seq in range(frames_count):
        active_points = min(parameter_pool, max(1, points_per_frame))
        parameter_ids = sorted(rng.sample(range(1, parameter_pool + 1), active_points))
        values: dict[str, FramePayload] = {
            f"p{parameter_id}": FramePayload(
                {
                    "parameter_id": parameter_id,
                    "value": round((parameter_id * 0.125) + rng.random(), 6),
                }
            )
            for parameter_id in parameter_ids
        }
        frames.append(
            IngestFrame(
                {
                    "seq": seq,
                    "ts_monotonic": base_ts + (seq * 0.125),
                    "values": values,
                }
            )
        )
    return frames


def _build_query_specs(
    dataset: list[Row], *, wide: bool
) -> list[tuple[int, int, set[int]]]:
    """Construct benchmark query windows and parameter subsets.

    Args:
        dataset: Source dataset used to derive time windows and IDs.
        wide: Whether to build wide scans and larger parameter sets.

    Returns:
        list[tuple[int, int, set[int]]]: Query specs in
            `(start_ts_ns, end_ts_ns, parameter_ids)` form.

    """
    timestamps = [timestamp_ns for timestamp_ns, _values in dataset]
    all_parameter_ids = sorted(
        {parameter_id for _ts, values in dataset for parameter_id, _ in values}
    )
    if wide:
        widths = [256, 384, 512, 768]
        parameter_sizes = [len(all_parameter_ids), max(6, len(all_parameter_ids) // 2)]
    else:
        widths = [8, 12, 16, 24]
        parameter_sizes = [1, 2, 3]

    specs: list[tuple[int, int, set[int]]] = []
    for index, width in enumerate(widths):
        start_index = min(
            len(timestamps) - 1, index * max(1, len(timestamps) // len(widths))
        )
        end_index = min(len(timestamps) - 1, start_index + width)
        start_ts_ns = timestamps[start_index]
        end_ts_ns = timestamps[end_index] + 1
        for size in parameter_sizes:
            specs.append((start_ts_ns, end_ts_ns, set(all_parameter_ids[:size])))
    return specs


def _stats_diff(before: StorageStats, after: StorageStats) -> dict[str, int]:
    """Compute per-field delta between two storage stats snapshots.

    Args:
        before: Baseline statistics snapshot.
        after: Statistics snapshot captured after a scenario.

    Returns:
        dict[str, int]: Signed field deltas keyed by stat field name.

    """
    return {
        field.name: int(getattr(after, field.name)) - int(getattr(before, field.name))
        for field in fields(StorageStats)
    }


def _percentile(values: list[float], percentile: int) -> float:
    """Return percentile from latency samples using inclusive quantiles.

    Args:
        values: Numeric samples.
        percentile: Percentile in the range 1..100.

    Returns:
        float: Computed percentile value, or `0.0` when input is empty.

    """
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return float(
        statistics.quantiles(values, n=100, method="inclusive")[percentile - 1]
    )
