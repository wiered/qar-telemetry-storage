"""CLI entrypoint for the QAR telemetry system (FDAU demo, ingest, query, aggregate, benchmark)."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping
from logging import getLogger
from pathlib import Path
import sys
import threading
import time
from typing import cast

_src_root = str(Path(__file__).resolve().parent)
if _src_root not in sys.path:
    sys.path.insert(0, _src_root)

import logging_config  # noqa: E402, F401

from core import FDAUUnit, FLTParser  # noqa: E402
from core.fdau import FDAUFrame  # noqa: E402
from core.ingest import IngestService  # noqa: E402
from core.storage import (  # noqa: E402
    aggregates_to_rows,
    points_to_rows,
    StorageCore,
    write_aggregates_csv,
    write_points_csv,
)
from core.storage.benchmark import (  # noqa: E402
    print_capture_human_summary,
    print_human_summary,
    print_scale_human_summary,
    run_benchmark_harness,
    run_capture_benchmark_harness,
    run_scale_benchmark_harness,
)
from core.storage.flight_plot import (  # noqa: E402
    generate_live_demo_flight_plot_points,
    generate_demo_flight_plot_points,
    show_flight_plot_points_window,
    show_flight_plot_window,
)
from settings import settings  # noqa: E402

logger = getLogger(__name__)


def fdau_printer(
    flt: FLTParser,
    *,
    seed: int,
    duration_sec: float,
    stress_mode: bool = False,
) -> None:
    """Run FDAU simulation and print each frame to stdout for the given duration."""

    def printer(frame: FDAUFrame) -> None:
        if frame:
            print(
                f"SEQ {frame['seq']}, TS {frame['ts']}, TS_MONOTONIC {frame['ts_monotonic']}, "
                f"Tick {frame['tick']}, Major frame {frame['major_frame']}: {frame['values']}"
            )

    fdau = FDAUUnit(flt.data, on_frame=printer, seed=seed, stress_mode=stress_mode)
    fdau.start()
    try:
        time.sleep(duration_sec)
    finally:
        fdau.stop()
        fdau.join(timeout=duration_sec)


def fdau_ingest(
    flt: FLTParser,
    *,
    seed: int,
    duration_sec: float,
    stress_mode: bool = False,
    print_stats: bool = False,
) -> None:
    """Run FDAU simulation feeding the ingest pipeline into storage for the given duration."""
    storage = StorageCore(settings=settings)
    ingest = IngestService(settings=settings, storage=storage)
    on_frame = cast(Callable[[FDAUFrame], None], ingest.on_frame)
    fdau = FDAUUnit(flt.data, on_frame=on_frame, seed=seed, stress_mode=stress_mode)

    stats_stop_event = threading.Event()

    def stats_printer() -> None:
        start_time = time.time()
        last_stats = {
            "frames_in": 0,
            "rows_out": 0,
            "points_out": 0,
            "dropped_frames": 0,
        }
        while not stats_stop_event.wait(1.0):
            stats = ingest.stats()
            elapsed = time.time() - start_time
            frames_delta = stats["frames_in"] - last_stats["frames_in"]
            rows_delta = stats["rows_out"] - last_stats["rows_out"]
            points_delta = stats["points_out"] - last_stats["points_out"]
            dropped_delta = stats["dropped_frames"] - last_stats["dropped_frames"]
            print(
                f"[Ingest Stats] t={elapsed:.1f}s | frames={stats['frames_in']} (+{frames_delta}/s) "
                f"rows={stats['rows_out']} (+{rows_delta}/s) "
                f"points={stats['points_out']} (+{points_delta}/s) "
                f"dropped={stats['dropped_frames']} (+{dropped_delta}/s) "
                f"queue={stats['queue_size']} seq_gaps={stats['seq_gaps']}"
            )
            last_stats = {
                "frames_in": stats["frames_in"],
                "rows_out": stats["rows_out"],
                "points_out": stats["points_out"],
                "dropped_frames": stats["dropped_frames"],
            }

    stats_thread = threading.Thread(
        target=stats_printer, daemon=True, name="StatsPrinter"
    )
    if print_stats:
        stats_thread.start()

    if stress_mode:
        logger.info("running ingest in stress mode")

    ingest.start()
    fdau.start()
    try:
        time.sleep(duration_sec)
    finally:
        if print_stats:
            stats_stop_event.set()
            stats_thread.join(timeout=1.0)

        fdau.stop()
        fdau.join(timeout=duration_sec)
        ingest.stop(join=True, timeout=duration_sec)
        storage.close()
        logger.info("final ingest stats: %s", ingest.stats())
        logger.info("storage stats: %s", storage.stats_snapshot())


def run_benchmark(args: argparse.Namespace) -> None:
    """Execute the storage benchmark harness from parsed CLI arguments."""
    output_json = Path(args.output_json) if args.output_json else None
    candidates = [int(value) for value in args.sweep.split(",") if value.strip()]
    report = run_benchmark_harness(
        seed=args.seed,
        rows_count=args.rows,
        parameter_pool=args.parameter_pool,
        points_per_row=args.points_per_row,
        block_max_points=args.block_max_points,
        sweep_candidates=candidates,
        output_json=output_json,
    )
    print_human_summary(report)
    if output_json is not None:
        print(f"\nJSON report: {output_json}")


def run_scale_benchmark(args: argparse.Namespace) -> None:
    """Execute the storage scale benchmark from parsed CLI arguments."""
    output_json = Path(args.output_json) if args.output_json else None
    parameter_pools = [int(value) for value in args.pools.split(",") if value.strip()]
    report = run_scale_benchmark_harness(
        seed=args.seed,
        rows_count=args.rows,
        parameter_pools=parameter_pools,
        block_max_points=args.block_max_points,
        output_json=output_json,
    )
    print_scale_human_summary(report)
    if output_json is not None:
        print(f"\nJSON report: {output_json}")


def run_capture_benchmark(args: argparse.Namespace) -> None:
    """Execute the end-to-end capture benchmark from parsed CLI arguments."""
    output_json = Path(args.output_json) if args.output_json else None
    report = run_capture_benchmark_harness(
        seed=args.seed,
        frames_count=args.frames,
        parameter_pool=args.parameter_pool,
        points_per_frame=args.points_per_frame,
        queue_max_frames=args.queue_max_frames,
        batch_max_rows=args.batch_max_rows,
        batch_max_points=args.batch_max_points,
        batch_max_ms=args.batch_max_ms,
        block_max_points=args.block_max_points,
        output_json=output_json,
    )
    print_capture_human_summary(report)
    if output_json is not None:
        print(f"\nJSON report: {output_json}")


def parse_parameter_ids(value: str) -> set[int] | None:
    """Parse a comma-separated list of parameter IDs, or None when empty."""
    if not value.strip():
        return None
    return {int(item.strip()) for item in value.split(",") if item.strip()}


def print_rows(rows: Iterable[Mapping[str, object]]) -> None:
    """Print tabular rows as comma-separated values to stdout."""
    for row in rows:
        print(",".join("" if value is None else str(value) for value in row.values()))


def run_query(args: argparse.Namespace) -> None:
    """Query stored points over a time range; optionally write CSV."""
    parameter_ids = parse_parameter_ids(args.parameter_ids)
    storage = StorageCore(settings=settings)
    try:
        points = storage.query_range(args.start_ts_ns, args.end_ts_ns, parameter_ids)
        if args.output_csv:
            write_points_csv(points, args.output_csv)
            print(f"CSV report: {args.output_csv}")
            return
        print_rows(points_to_rows(points))
    finally:
        storage.close()


def run_aggregate(args: argparse.Namespace) -> None:
    """Aggregate stored points over a time range; optionally write CSV."""
    parameter_ids = parse_parameter_ids(args.parameter_ids)
    storage = StorageCore(settings=settings)
    try:
        results = storage.aggregate_range(
            args.start_ts_ns,
            args.end_ts_ns,
            parameter_ids,
        )
        if args.output_csv:
            write_aggregates_csv(results, args.output_csv)
            print(f"CSV report: {args.output_csv}")
            return
        print_rows(aggregates_to_rows(results))
    finally:
        storage.close()


def run_plot_flight(args: argparse.Namespace) -> None:
    """Open an interactive matplotlib window with 10 telemetry series."""
    flt_parser = FLTParser(args.flt_file)
    storage = StorageCore(settings=settings)
    try:
        show_flight_plot_window(
            storage,
            flt_parser.data,
            target_seconds=args.target_seconds,
        )
    finally:
        storage.close()


def run_plot_flight_demo(args: argparse.Namespace) -> None:
    """Open an interactive matplotlib demo window with synthetic linear series."""
    flt_parser = FLTParser(args.flt_file)
    points = generate_demo_flight_plot_points(
        flt_parser.data,
        target_seconds=args.target_seconds,
        samples_per_second=args.samples_per_second,
    )
    show_flight_plot_points_window(
        points,
        flt_parser.data,
        target_seconds=args.target_seconds,
        title_prefix="Flight Telemetry Demo (linear debug series)",
    )


def run_plot_flight_live(args: argparse.Namespace) -> None:
    """Open an interactive matplotlib window with a synthetic takeoff demo."""
    flt_parser = FLTParser(args.flt_file)
    points = generate_live_demo_flight_plot_points(
        flt_parser.data,
        target_seconds=120.0,
        samples_per_second=args.samples_per_second,
    )
    show_flight_plot_points_window(
        points,
        flt_parser.data,
        target_seconds=120.0,
        title_prefix="Flight Telemetry Live Demo (takeoff profile)",
    )


def build_parser() -> argparse.ArgumentParser:
    """Construct the root argparse parser with printer, ingest, query, aggregate, and benchmark subcommands."""
    parser = argparse.ArgumentParser(description="QAR telemetry system")
    subparsers = parser.add_subparsers(dest="command", required=True)

    printer_parser = subparsers.add_parser("printer", help="print FDAU frames")
    printer_parser.add_argument("--duration", type=float, default=3.0)
    printer_parser.add_argument("--seed", type=int, default=42)
    printer_parser.add_argument("--stress", action="store_true")

    ingest_parser = subparsers.add_parser("ingest", help="run ingest with real storage")
    ingest_parser.add_argument("--duration", type=float, default=10.0)
    ingest_parser.add_argument("--seed", type=int, default=42)
    ingest_parser.add_argument("--stress", action="store_true")
    ingest_parser.add_argument("--print-stats", action="store_true")

    query_parser = subparsers.add_parser("query", help="query stored points")
    query_parser.add_argument("--start-ts-ns", type=int, required=True)
    query_parser.add_argument("--end-ts-ns", type=int, required=True)
    query_parser.add_argument("--parameter-ids", type=str, default="")
    query_parser.add_argument("--output-csv", type=str, default="")

    aggregate_parser = subparsers.add_parser(
        "aggregate", help="aggregate stored points"
    )
    aggregate_parser.add_argument("--start-ts-ns", type=int, required=True)
    aggregate_parser.add_argument("--end-ts-ns", type=int, required=True)
    aggregate_parser.add_argument("--parameter-ids", type=str, default="")
    aggregate_parser.add_argument("--output-csv", type=str, default="")

    plot_parser = subparsers.add_parser(
        "plot-flight",
        help="open a matplotlib window with 10 telemetry series",
    )
    plot_parser.add_argument("--target-seconds", type=float, required=True)
    plot_parser.add_argument("--flt-file", type=str, default="data/base_flt.json")

    plot_demo_parser = subparsers.add_parser(
        "plot-flight-demo",
        help="open a matplotlib window with synthetic linear telemetry for layout debugging",
    )
    plot_demo_parser.add_argument("--target-seconds", type=float, required=True)
    plot_demo_parser.add_argument("--samples-per-second", type=int, default=4)
    plot_demo_parser.add_argument("--flt-file", type=str, default="data/base_flt.json")

    plot_live_parser = subparsers.add_parser(
        "plot-flight-live",
        help="open a matplotlib window with a synthetic 120-second takeoff demo",
    )
    plot_live_parser.add_argument("--samples-per-second", type=int, default=4)
    plot_live_parser.add_argument("--flt-file", type=str, default="data/base_flt.json")

    benchmark_parser = subparsers.add_parser(
        "benchmark", help="run storage benchmark harness"
    )
    benchmark_parser.add_argument("--seed", type=int, default=42)
    benchmark_parser.add_argument("--rows", type=int, default=4_096)
    benchmark_parser.add_argument("--parameter-pool", type=int, default=32)
    benchmark_parser.add_argument("--points-per-row", type=int, default=8)
    benchmark_parser.add_argument(
        "--block-max-points",
        type=int,
        default=settings.storage.sstable_block_max_points,
    )
    benchmark_parser.add_argument("--sweep", type=str, default="256,512,1024,2048,4096")
    benchmark_parser.add_argument("--output-json", type=str, default="")

    scale_parser = subparsers.add_parser(
        "benchmark-scale", help="run storage scale benchmark for FDAU-sized pools"
    )
    scale_parser.add_argument("--seed", type=int, default=42)
    scale_parser.add_argument("--rows", type=int, default=512)
    scale_parser.add_argument("--pools", type=str, default="200,500,1000,3000")
    scale_parser.add_argument(
        "--block-max-points",
        type=int,
        default=settings.storage.sstable_block_max_points,
    )
    scale_parser.add_argument("--output-json", type=str, default="")

    capture_parser = subparsers.add_parser(
        "benchmark-capture",
        help="run end-to-end capture benchmark for ingest and storage metrics",
    )
    capture_parser.add_argument("--seed", type=int, default=42)
    capture_parser.add_argument("--frames", type=int, default=512)
    capture_parser.add_argument("--parameter-pool", type=int, default=3_000)
    capture_parser.add_argument("--points-per-frame", type=int, default=3_000)
    capture_parser.add_argument("--queue-max-frames", type=int, default=10_000)
    capture_parser.add_argument("--batch-max-rows", type=int, default=1_000)
    capture_parser.add_argument("--batch-max-points", type=int, default=50_000)
    capture_parser.add_argument("--batch-max-ms", type=float, default=50.0)
    capture_parser.add_argument(
        "--block-max-points",
        type=int,
        default=settings.storage.sstable_block_max_points,
    )
    capture_parser.add_argument("--output-json", type=str, default="")

    return parser


def main() -> None:
    """Parse CLI arguments and dispatch to the selected subcommand."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "printer":
        flt_parser = FLTParser("data/base_flt.json")
        fdau_printer(
            flt_parser,
            seed=args.seed,
            duration_sec=args.duration,
            stress_mode=args.stress,
        )
        return
    if args.command == "ingest":
        flt_parser = FLTParser("data/base_flt.json")
        fdau_ingest(
            flt_parser,
            seed=args.seed,
            duration_sec=args.duration,
            stress_mode=args.stress,
            print_stats=args.print_stats,
        )
        return
    if args.command == "query":
        run_query(args)
        return
    if args.command == "aggregate":
        run_aggregate(args)
        return
    if args.command == "plot-flight":
        run_plot_flight(args)
        return
    if args.command == "plot-flight-demo":
        run_plot_flight_demo(args)
        return
    if args.command == "plot-flight-live":
        run_plot_flight_live(args)
        return
    if args.command == "benchmark":
        run_benchmark(args)
        return
    if args.command == "benchmark-scale":
        run_scale_benchmark(args)
        return
    if args.command == "benchmark-capture":
        run_capture_benchmark(args)
        return
    parser.error(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
