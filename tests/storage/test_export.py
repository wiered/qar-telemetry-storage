"""Tests for CSV export helpers (points and aggregates)."""

from __future__ import annotations

import csv
from io import StringIO

from core.storage import (
    AGGREGATE_CSV_COLUMNS,
    POINT_CSV_COLUMNS,
    AggregateResult,
    Point,
    aggregates_to_rows,
    points_to_rows,
    write_aggregates_csv,
    write_points_csv,
)


def read_csv(text: str) -> tuple[list[str] | None, list[dict[str, str]]]:
    """Parse CSV text into header field names and row mappings.

    Args:
        text: Raw CSV body.

    Returns:
        DictReader fieldnames and list of row dicts.
    """
    reader = csv.DictReader(StringIO(text))
    fieldnames = list(reader.fieldnames) if reader.fieldnames is not None else None
    return fieldnames, list(reader)


def test_points_to_rows_returns_plain_dicts_with_stable_key_order():
    """Rows use ``POINT_CSV_COLUMNS`` order and plain dicts."""
    points = [
        Point(timestamp_ns=100, parameter_id=2, value=10.5),
        Point(timestamp_ns=101, parameter_id=1, value=-3.25),
    ]

    rows = points_to_rows(points)

    assert rows == [
        {
            "timestamp_ns": 100,
            "parameter_id": 2,
            "value": 10.5,
        },
        {
            "timestamp_ns": 101,
            "parameter_id": 1,
            "value": -3.25,
        },
    ]
    assert all(isinstance(row, dict) for row in rows)
    assert [row["parameter_id"] for row in rows] == [2, 1]
    assert all(list(row.keys()) == POINT_CSV_COLUMNS for row in rows)


def test_points_to_rows_returns_empty_list_for_empty_input():
    """Empty input yields an empty list."""
    assert points_to_rows([]) == []


def test_aggregates_to_rows_returns_plain_dicts_with_stable_key_order():
    """Rows use ``AGGREGATE_CSV_COLUMNS`` order and plain dicts."""
    results = [
        AggregateResult(
            start_ts_ns=100,
            end_ts_ns=200,
            parameter_id=2,
            count=2,
            min=1.0,
            max=3.0,
            avg=2.0,
        ),
        AggregateResult(
            start_ts_ns=100,
            end_ts_ns=200,
            parameter_id=1,
            count=0,
        ),
    ]

    rows = aggregates_to_rows(results)

    assert rows == [result.to_row() for result in results]
    assert all(isinstance(row, dict) for row in rows)
    assert [row["parameter_id"] for row in rows] == [2, 1]
    assert all(list(row.keys()) == AGGREGATE_CSV_COLUMNS for row in rows)


def test_aggregates_to_rows_returns_empty_list_for_empty_input():
    """Empty input yields an empty list."""
    assert aggregates_to_rows([]) == []


def test_write_points_csv_to_file_like_writes_header_and_rows():
    """Buffered writer receives header and numeric string cells."""
    target = StringIO()

    write_points_csv(
        [
            Point(timestamp_ns=100, parameter_id=2, value=10.5),
            Point(timestamp_ns=101, parameter_id=1, value=-3.25),
        ],
        target,
    )

    assert not target.closed
    fieldnames, rows = read_csv(target.getvalue())
    assert fieldnames == POINT_CSV_COLUMNS
    assert rows == [
        {
            "timestamp_ns": "100",
            "parameter_id": "2",
            "value": "10.5",
        },
        {
            "timestamp_ns": "101",
            "parameter_id": "1",
            "value": "-3.25",
        },
    ]


def test_write_points_csv_to_path_writes_readable_csv(tmp_path):
    """Path target produces a UTF-8 CSV file with expected columns."""
    csv_path = tmp_path / "points.csv"

    write_points_csv(
        [Point(timestamp_ns=100, parameter_id=2, value=10.5)],
        csv_path,
    )

    assert csv_path.exists()
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    assert reader.fieldnames == POINT_CSV_COLUMNS
    assert rows == [
        {
            "timestamp_ns": "100",
            "parameter_id": "2",
            "value": "10.5",
        }
    ]


def test_write_points_csv_empty_input_writes_only_header():
    """No data rows still emits the canonical header."""
    target = StringIO()

    write_points_csv([], target)

    fieldnames, rows = read_csv(target.getvalue())
    assert fieldnames == POINT_CSV_COLUMNS
    assert rows == []


def test_write_aggregates_csv_to_file_like_writes_header_and_rows():
    """Buffered writer receives aggregate header and empty-string nulls."""
    target = StringIO()

    write_aggregates_csv(
        [
            AggregateResult(
                start_ts_ns=100,
                end_ts_ns=200,
                parameter_id=2,
                count=2,
                min=1.0,
                max=3.0,
                avg=2.0,
            ),
            AggregateResult(
                start_ts_ns=100,
                end_ts_ns=200,
                parameter_id=1,
                count=0,
            ),
        ],
        target,
    )

    assert not target.closed
    fieldnames, rows = read_csv(target.getvalue())
    assert fieldnames == AGGREGATE_CSV_COLUMNS
    assert rows == [
        {
            "start_ts_ns": "100",
            "end_ts_ns": "200",
            "parameter_id": "2",
            "count": "2",
            "min": "1.0",
            "max": "3.0",
            "avg": "2.0",
        },
        {
            "start_ts_ns": "100",
            "end_ts_ns": "200",
            "parameter_id": "1",
            "count": "0",
            "min": "",
            "max": "",
            "avg": "",
        },
    ]


def test_write_aggregates_csv_to_path_writes_readable_csv(tmp_path):
    """Path target produces a UTF-8 CSV file with expected columns."""
    csv_path = tmp_path / "aggregates.csv"

    write_aggregates_csv(
        [
            AggregateResult(
                start_ts_ns=100,
                end_ts_ns=200,
                parameter_id=2,
                count=2,
                min=1.0,
                max=3.0,
                avg=2.0,
            )
        ],
        csv_path,
    )

    assert csv_path.exists()
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    assert reader.fieldnames == AGGREGATE_CSV_COLUMNS
    assert rows == [
        {
            "start_ts_ns": "100",
            "end_ts_ns": "200",
            "parameter_id": "2",
            "count": "2",
            "min": "1.0",
            "max": "3.0",
            "avg": "2.0",
        }
    ]


def test_write_aggregates_csv_empty_input_writes_only_header():
    """No data rows still emits the canonical header."""
    target = StringIO()

    write_aggregates_csv([], target)

    fieldnames, rows = read_csv(target.getvalue())
    assert fieldnames == AGGREGATE_CSV_COLUMNS
    assert rows == []


def test_export_api_is_available_from_public_storage_package():
    """Export helpers and column constants match ``core.storage`` exports."""
    import core.storage as storage_package

    assert storage_package.points_to_rows is points_to_rows
    assert storage_package.write_points_csv is write_points_csv
    assert storage_package.aggregates_to_rows is aggregates_to_rows
    assert storage_package.write_aggregates_csv is write_aggregates_csv
    assert storage_package.POINT_CSV_COLUMNS == [
        "timestamp_ns",
        "parameter_id",
        "value",
    ]
    assert storage_package.AGGREGATE_CSV_COLUMNS == [
        "start_ts_ns",
        "end_ts_ns",
        "parameter_id",
        "count",
        "min",
        "max",
        "avg",
    ]
