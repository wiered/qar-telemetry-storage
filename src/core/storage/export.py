"""CSV export helpers for point streams and aggregate query results."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping
from os import PathLike
from pathlib import Path
from typing import IO, cast

from .analysis import AggregateResult, AggregateRow, aggregate_results_to_rows
from .models import Point

PointRow = dict[str, int | float]
CsvTarget = str | PathLike[str] | IO[str]

POINT_CSV_COLUMNS = [
    "timestamp_ns",
    "parameter_id",
    "value",
]

AGGREGATE_CSV_COLUMNS = [
    "start_ts_ns",
    "end_ts_ns",
    "parameter_id",
    "count",
    "min",
    "max",
    "avg",
]


def points_to_rows(points: Iterable[Point]) -> list[PointRow]:
    """Convert points to CSV-compatible dictionaries.

    Args:
        points: Source points to serialize.

    Returns:
        List of point rows ready for CSV export.
    """
    return [point.to_dict() for point in points]


def aggregates_to_rows(results: Iterable[AggregateResult]) -> list[AggregateRow]:
    """Convert aggregate results to CSV-compatible dictionaries.

    Args:
        results: Aggregate computation results.

    Returns:
        List of aggregate rows ready for CSV export.
    """
    return aggregate_results_to_rows(results)


def write_points_csv(points: Iterable[Point], file_or_path: CsvTarget) -> None:
    """Write points to CSV file with standard point columns.

    Args:
        points: Source points to write.
        file_or_path: Path or opened text stream for CSV output.
    """
    _write_csv_rows(points_to_rows(points), POINT_CSV_COLUMNS, file_or_path)


def write_aggregates_csv(
    results: Iterable[AggregateResult],
    file_or_path: CsvTarget,
) -> None:
    """Write aggregate rows to CSV file with standard aggregate columns.

    Args:
        results: Aggregate computation results to write.
        file_or_path: Path or opened text stream for CSV output.
    """
    _write_csv_rows(aggregates_to_rows(results), AGGREGATE_CSV_COLUMNS, file_or_path)


def _write_csv_rows(
    rows: Iterable[Mapping[str, object]],
    fieldnames: list[str],
    file_or_path: CsvTarget,
) -> None:
    """Write row mappings as CSV to path or stream.

    Args:
        rows: Row mappings keyed by column name.
        fieldnames: Output column order.
        file_or_path: Destination path or opened text stream.
    """
    if isinstance(file_or_path, str | PathLike):
        path = cast(str | PathLike[str], file_or_path)
        with Path(path).open("w", newline="", encoding="utf-8") as csv_file:
            _write_csv_rows_to_file(rows, fieldnames, csv_file)
        return

    _write_csv_rows_to_file(rows, fieldnames, file_or_path)


def _write_csv_rows_to_file(
    rows: Iterable[Mapping[str, object]],
    fieldnames: list[str],
    csv_file: IO[str],
) -> None:
    """Write CSV rows to an already opened text file.

    Args:
        rows: Row mappings keyed by column name.
        fieldnames: Output column order.
        csv_file: Opened writable text stream.
    """
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
