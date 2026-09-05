"""Tests for ``main`` CLI parsing and ``query`` / ``aggregate`` commands."""

import argparse
from types import SimpleNamespace

import main
from core.storage import StorageCore, StorageRuntimeConfig


def make_config(tmp_path, **overrides):
    """Return ``StorageRuntimeConfig`` rooted under ``tmp_path`` with optional overrides."""
    return StorageRuntimeConfig(
        data_dir=tmp_path / "storage",
        flush_max_rows=overrides.get("flush_max_rows", 100),
        flush_max_points=overrides.get("flush_max_points", 100),
        flush_max_bytes=overrides.get("flush_max_bytes", 100_000),
        sstable_block_max_points=overrides.get("sstable_block_max_points", 2),
        compaction_min_tables=overrides.get("compaction_min_tables", 4),
        sstable_format=overrides.get("sstable_format", "v2_timeseries"),
        cleanup_temp_on_startup=overrides.get("cleanup_temp_on_startup", True),
        quarantine_dir_name=overrides.get("quarantine_dir_name", "quarantine"),
    )


def make_settings(config):
    """Build a minimal namespace shaped like ``Settings`` from ``StorageRuntimeConfig``."""
    return SimpleNamespace(
        data_dir=config.data_dir,
        storage=SimpleNamespace(
            flush_max_rows=config.flush_max_rows,
            flush_max_points=config.flush_max_points,
            flush_max_bytes=config.flush_max_bytes,
            sstable_block_max_points=config.sstable_block_max_points,
            compaction_min_tables=config.compaction_min_tables,
            sstable_format=config.sstable_format,
            cleanup_temp_on_startup=config.cleanup_temp_on_startup,
            quarantine_dir_name=config.quarantine_dir_name,
        ),
    )


def seed_storage(config):
    """Append sample rows and close storage so CLI commands read deterministic data."""
    storage = StorageCore(config=config)
    storage.append_rows(
        [
            (100, [(1, 1.0), (2, 10.0)]),
            (110, [(1, 3.0)]),
            (120, [(2, 20.0)]),
        ]
    )
    storage.close()


def test_parser_accepts_query_and_aggregate_commands():
    """CLI parser accepts ``query`` and ``aggregate`` subcommands with expected flags."""
    parser = main.build_parser()

    query_args = parser.parse_args(
        [
            "query",
            "--start-ts-ns",
            "100",
            "--end-ts-ns",
            "120",
            "--parameter-ids",
            "1,2",
        ]
    )
    aggregate_args = parser.parse_args(
        [
            "aggregate",
            "--start-ts-ns",
            "100",
            "--end-ts-ns",
            "120",
            "--output-csv",
            "aggregates.csv",
        ]
    )

    assert query_args.command == "query"
    assert query_args.start_ts_ns == 100
    assert query_args.end_ts_ns == 120
    assert query_args.parameter_ids == "1,2"
    assert aggregate_args.command == "aggregate"
    assert aggregate_args.output_csv == "aggregates.csv"


def test_parser_accepts_benchmark_scale_command():
    """CLI parser accepts scale benchmark pools and output path."""
    parser = main.build_parser()

    args = parser.parse_args(
        [
            "benchmark-scale",
            "--rows",
            "256",
            "--pools",
            "200,500",
            "--output-json",
            "reports/scale.json",
        ]
    )

    assert args.command == "benchmark-scale"
    assert args.rows == 256
    assert args.pools == "200,500"
    assert args.output_json == "reports/scale.json"


def test_parser_accepts_benchmark_capture_command():
    """CLI parser accepts capture benchmark options."""
    parser = main.build_parser()

    args = parser.parse_args(
        [
            "benchmark-capture",
            "--frames",
            "256",
            "--parameter-pool",
            "500",
            "--points-per-frame",
            "250",
            "--output-json",
            "reports/capture.json",
        ]
    )

    assert args.command == "benchmark-capture"
    assert args.frames == 256
    assert args.parameter_pool == 500
    assert args.points_per_frame == 250
    assert args.output_json == "reports/capture.json"


def test_parser_accepts_plot_flight_command():
    """CLI parser accepts the interactive telemetry plotting command."""
    parser = main.build_parser()

    args = parser.parse_args(
        [
            "plot-flight",
            "--target-seconds",
            "120",
            "--flt-file",
            "data/base_flt.json",
        ]
    )

    assert args.command == "plot-flight"
    assert args.target_seconds == 120
    assert args.flt_file == "data/base_flt.json"


def test_parser_accepts_plot_flight_demo_command():
    """CLI parser accepts the synthetic demo plotting command."""
    parser = main.build_parser()

    args = parser.parse_args(
        [
            "plot-flight-demo",
            "--target-seconds",
            "45",
            "--samples-per-second",
            "8",
        ]
    )

    assert args.command == "plot-flight-demo"
    assert args.target_seconds == 45
    assert args.samples_per_second == 8


def test_parser_accepts_plot_flight_live_command():
    """CLI parser accepts the presentation takeoff plotting command."""
    parser = main.build_parser()

    args = parser.parse_args(
        [
            "plot-flight-live",
            "--samples-per-second",
            "6",
        ]
    )

    assert args.command == "plot-flight-live"
    assert args.samples_per_second == 6


def test_parse_parameter_ids_returns_none_for_empty_value():
    """Empty or whitespace ``parameter_ids`` yields ``None``; otherwise parses id sets."""
    assert main.parse_parameter_ids("") is None
    assert main.parse_parameter_ids("  ") is None
    assert main.parse_parameter_ids("1, 2,3") == {1, 2, 3}


def test_query_command_prints_point_rows(tmp_path, monkeypatch, capsys):
    """``run_query`` prints CSV rows to stdout when ``output_csv`` is empty."""
    config = make_config(tmp_path)
    seed_storage(config)
    monkeypatch.setattr(main, "settings", make_settings(config))

    main.run_query(
        argparse.Namespace(
            start_ts_ns=100,
            end_ts_ns=120,
            parameter_ids="1",
            output_csv="",
        )
    )

    assert capsys.readouterr().out.splitlines() == [
        "100,1,1.0",
        "110,1,3.0",
    ]


def test_query_command_writes_points_csv(tmp_path, monkeypatch, capsys):
    """``run_query`` writes a points CSV file and echoes its path when requested."""
    config = make_config(tmp_path)
    seed_storage(config)
    csv_path = tmp_path / "points.csv"
    monkeypatch.setattr(main, "settings", make_settings(config))

    main.run_query(
        argparse.Namespace(
            start_ts_ns=100,
            end_ts_ns=121,
            parameter_ids="2",
            output_csv=str(csv_path),
        )
    )

    assert capsys.readouterr().out.strip() == f"CSV report: {csv_path}"
    assert csv_path.read_text(encoding="utf-8").splitlines() == [
        "timestamp_ns,parameter_id,value",
        "100,2,10.0",
        "120,2,20.0",
    ]


def test_aggregate_command_prints_aggregate_rows(tmp_path, monkeypatch, capsys):
    """``run_aggregate`` prints aggregate rows to stdout when ``output_csv`` is empty."""
    config = make_config(tmp_path)
    seed_storage(config)
    monkeypatch.setattr(main, "settings", make_settings(config))

    main.run_aggregate(
        argparse.Namespace(
            start_ts_ns=100,
            end_ts_ns=120,
            parameter_ids="1",
            output_csv="",
        )
    )

    assert capsys.readouterr().out.splitlines() == [
        "100,120,1,2,1.0,3.0,2.0",
    ]


def test_aggregate_command_writes_aggregates_csv(tmp_path, monkeypatch, capsys):
    """``run_aggregate`` writes an aggregates CSV and echoes its path when requested."""
    config = make_config(tmp_path)
    seed_storage(config)
    csv_path = tmp_path / "aggregates.csv"
    monkeypatch.setattr(main, "settings", make_settings(config))

    main.run_aggregate(
        argparse.Namespace(
            start_ts_ns=100,
            end_ts_ns=120,
            parameter_ids="1,3",
            output_csv=str(csv_path),
        )
    )

    assert capsys.readouterr().out.strip() == f"CSV report: {csv_path}"
    assert csv_path.read_text(encoding="utf-8").splitlines() == [
        "start_ts_ns,end_ts_ns,parameter_id,count,min,max,avg",
        "100,120,1,2,1.0,3.0,2.0",
        "100,120,3,0,,,",
    ]


def test_plot_flight_command_calls_window_builder(tmp_path, monkeypatch):
    """``run_plot_flight`` loads FLT, storage, and plotting function."""
    config = make_config(tmp_path)
    seed_storage(config)
    monkeypatch.setattr(main, "settings", make_settings(config))

    recorded = {}

    def fake_show(storage, flt_data, *, target_seconds):
        recorded["storage"] = storage
        recorded["flt_data"] = flt_data
        recorded["target_seconds"] = target_seconds

    monkeypatch.setattr(main, "show_flight_plot_window", fake_show)

    main.run_plot_flight(
        argparse.Namespace(
            target_seconds=30.0,
            flt_file="data/base_flt.json",
        )
    )

    assert recorded["target_seconds"] == 30.0
    assert recorded["flt_data"].layout


def test_plot_flight_demo_command_calls_point_window_builder(monkeypatch):
    """``run_plot_flight_demo`` generates demo points and forwards them to plotting."""
    recorded = {}

    def fake_show(points, flt_data, *, target_seconds, title_prefix):
        recorded["points"] = points
        recorded["flt_data"] = flt_data
        recorded["target_seconds"] = target_seconds
        recorded["title_prefix"] = title_prefix

    monkeypatch.setattr(main, "show_flight_plot_points_window", fake_show)

    main.run_plot_flight_demo(
        argparse.Namespace(
            target_seconds=12.0,
            samples_per_second=5,
            flt_file="data/base_flt.json",
        )
    )

    assert recorded["target_seconds"] == 12.0
    assert recorded["title_prefix"] == "Flight Telemetry Demo (linear debug series)"
    assert recorded["flt_data"].layout
    assert recorded["points"]


def test_plot_flight_live_command_calls_point_window_builder(monkeypatch):
    """``run_plot_flight_live`` generates takeoff demo points and plots them."""
    recorded = {}

    def fake_show(points, flt_data, *, target_seconds, title_prefix):
        recorded["points"] = points
        recorded["flt_data"] = flt_data
        recorded["target_seconds"] = target_seconds
        recorded["title_prefix"] = title_prefix

    monkeypatch.setattr(main, "show_flight_plot_points_window", fake_show)

    main.run_plot_flight_live(
        argparse.Namespace(
            samples_per_second=4,
            flt_file="data/base_flt.json",
        )
    )

    assert recorded["target_seconds"] == 120.0
    assert recorded["title_prefix"] == "Flight Telemetry Live Demo (takeoff profile)"
    assert recorded["flt_data"].layout
    assert recorded["points"]
