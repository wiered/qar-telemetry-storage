"""Tests for aggregate analysis models and row serialization."""

from core.storage import AggregateFunction, AggregateResult, aggregate_results_to_rows

AGGREGATE_ROW_KEYS = [
    "start_ts_ns",
    "end_ts_ns",
    "parameter_id",
    "count",
    "min",
    "max",
    "avg",
]


def test_aggregate_function_values_are_stable():
    """Enum string values stay aligned with wire/API names."""
    assert AggregateFunction.MIN.value == "min"
    assert AggregateFunction.MAX.value == "max"
    assert AggregateFunction.AVG.value == "avg"
    assert AggregateFunction.COUNT.value == "count"


def test_aggregate_result_serializes_non_empty_row_with_stable_key_order():
    """``to_row()`` matches the canonical aggregate column order."""
    result = AggregateResult(
        start_ts_ns=100,
        end_ts_ns=200,
        parameter_id=7,
        count=3,
        min=1.5,
        max=4.5,
        avg=3.0,
    )

    row = result.to_row()

    assert row == {
        "start_ts_ns": 100,
        "end_ts_ns": 200,
        "parameter_id": 7,
        "count": 3,
        "min": 1.5,
        "max": 4.5,
        "avg": 3.0,
    }
    assert list(row.keys()) == AGGREGATE_ROW_KEYS


def test_empty_aggregate_result_uses_zero_count_and_none_values():
    """Zero-count aggregates omit min/max/avg (None)."""
    result = AggregateResult(
        start_ts_ns=100,
        end_ts_ns=200,
        parameter_id=7,
        count=0,
    )

    assert result.count == 0
    assert result.min is None
    assert result.max is None
    assert result.avg is None
    assert result.to_row() == {
        "start_ts_ns": 100,
        "end_ts_ns": 200,
        "parameter_id": 7,
        "count": 0,
        "min": None,
        "max": None,
        "avg": None,
    }


def test_aggregate_results_to_rows_returns_plain_dicts_in_input_order():
    """Batch helper preserves input order and emits plain dict rows."""
    results = [
        AggregateResult(
            start_ts_ns=100,
            end_ts_ns=200,
            parameter_id=7,
            count=3,
            min=1.5,
            max=4.5,
            avg=3.0,
        ),
        AggregateResult(
            start_ts_ns=100,
            end_ts_ns=200,
            parameter_id=8,
            count=0,
        ),
    ]

    rows = aggregate_results_to_rows(results)

    assert isinstance(rows, list)
    assert all(isinstance(row, dict) for row in rows)
    assert [row["parameter_id"] for row in rows] == [7, 8]
    assert all(list(row.keys()) == AGGREGATE_ROW_KEYS for row in rows)


def test_analysis_models_are_available_from_public_storage_package():
    """Public package exports align with aggregate helpers."""
    result = AggregateResult(
        start_ts_ns=1,
        end_ts_ns=2,
        parameter_id=3,
        count=1,
        min=4.0,
        max=4.0,
        avg=4.0,
    )

    assert AggregateFunction.COUNT.value == "count"
    assert aggregate_results_to_rows([result]) == [result.to_row()]
