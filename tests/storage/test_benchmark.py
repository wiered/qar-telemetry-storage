"""Tests for the storage micro-benchmark harness."""

import json

from core.storage.benchmark import (
    run_benchmark_harness,
    run_capture_benchmark_harness,
    run_scale_benchmark_harness,
)


def test_benchmark_harness_emits_json_and_required_counters(tmp_path):
    """Harness writes JSON with dataset metadata, runs, and sweep selection."""
    report_path = tmp_path / "benchmark.json"

    report = run_benchmark_harness(
        seed=7,
        rows_count=128,
        parameter_pool=8,
        points_per_row=4,
        block_max_points=64,
        sweep_candidates=[32, 64],
        output_json=report_path,
    )

    assert report_path.exists()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert "environment" in persisted
    assert persisted["parameters"]["sweep_candidates"] == [32, 64]
    assert persisted["dataset"]["rows"] == 128
    assert {run["sstable_format"] for run in report["runs"]} == {
        "v1_raw",
        "v2_timeseries",
    }
    narrow_query = report["runs"][1]["scenarios"]["narrow_query"]
    for counter_name in (
        "files_considered",
        "files_pruned",
        "files_opened",
        "blocks_considered",
        "blocks_pruned",
        "blocks_scanned",
        "points_returned",
    ):
        assert counter_name in narrow_query
    assert report["sweep"]["selected_block_max_points"] in {32, 64}


def test_scale_benchmark_harness_emits_pool_runs(tmp_path):
    """Scale harness writes one v2 run per requested parameter pool."""
    report_path = tmp_path / "scale-benchmark.json"

    report = run_scale_benchmark_harness(
        seed=7,
        rows_count=32,
        parameter_pools=[16, 32],
        block_max_points=64,
        output_json=report_path,
    )

    assert report_path.exists()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["parameters"]["parameter_pools"] == [16, 32]
    assert persisted["parameters"]["sstable_format"] == "v2_timeseries"
    assert [run["parameter_pool"] for run in report["runs"]] == [16, 32]
    for run in report["runs"]:
        assert run["points_per_row_target"] == run["parameter_pool"]
        assert {"ingest_only", "wide_query", "cold_recovery_startup"} == set(
            run["scenarios"]
        )
        assert run["scenarios"]["ingest_only"]["points_per_sec"] > 0


def test_capture_benchmark_harness_reports_four_core_metrics(tmp_path):
    """Capture harness reports rate, loss, latency, and throughput metrics."""
    report_path = tmp_path / "capture-benchmark.json"

    report = run_capture_benchmark_harness(
        seed=7,
        frames_count=64,
        parameter_pool=16,
        points_per_frame=8,
        queue_max_frames=128,
        batch_max_rows=16,
        batch_max_points=128,
        batch_max_ms=1000,
        block_max_points=64,
        output_json=report_path,
    )

    assert report_path.exists()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["parameters"]["sstable_format"] == "v2_timeseries"
    metrics = report["metrics"]
    for metric_name in (
        "end_to_end_capture_points_per_sec",
        "data_loss_rate",
        "write_latency_p95_ms",
        "write_latency_p99_ms",
        "write_throughput_points_per_sec",
    ):
        assert metric_name in metrics
    assert metrics["generated_points"] == 512
    assert metrics["points_out"] == 512
    assert metrics["data_loss_rate"] == 0
    assert metrics["append_calls"] > 0
