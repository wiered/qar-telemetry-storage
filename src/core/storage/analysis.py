"""Aggregation helpers for range-query analytics."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .models import Point

if TYPE_CHECKING:
    from .core import StorageCore

AggregateRow = dict[str, int | float | None]


class AggregateFunction(str, Enum):
    """Supported aggregate operations for analytical queries."""

    MIN = "min"
    MAX = "max"
    AVG = "avg"
    COUNT = "count"


@dataclass(frozen=True, slots=True)
class AggregateResult:
    """Normalized aggregate values for a single parameter and time window."""

    start_ts_ns: int
    end_ts_ns: int
    parameter_id: int
    count: int
    min: float | None = None
    max: float | None = None
    avg: float | None = None

    def __post_init__(self) -> None:
        """Validate invariants between `count` and aggregate fields."""
        if self.count < 0:
            raise ValueError("count must be non-negative")
        if self.count == 0 and (
            self.min is not None or self.max is not None or self.avg is not None
        ):
            raise ValueError("empty aggregate result must not contain min, max, or avg")
        if self.count > 0 and (
            self.min is None or self.max is None or self.avg is None
        ):
            raise ValueError(
                "non-empty aggregate result must contain min, max, and avg"
            )

    def to_row(self) -> AggregateRow:
        """Convert the result to a serialization-friendly row mapping.

        Returns:
            AggregateRow: Dictionary representation of the aggregate result.

        """
        return {
            "start_ts_ns": self.start_ts_ns,
            "end_ts_ns": self.end_ts_ns,
            "parameter_id": self.parameter_id,
            "count": self.count,
            "min": self.min,
            "max": self.max,
            "avg": self.avg,
        }

    def to_dict(self) -> AggregateRow:
        """Return dictionary representation of the aggregate result.

        Returns:
            AggregateRow: Same payload as `to_row()`.

        """
        return self.to_row()


@dataclass(slots=True)
class _AggregateAccumulator:
    """Mutable accumulator for one parameter while scanning points."""

    count: int = 0
    total: float = 0.0
    min_value: float | None = None
    max_value: float | None = None

    def add(self, value: float) -> None:
        """Add one point value to running aggregate state.

        Args:
            value: Point value to include in aggregate statistics.

        """
        self.count += 1
        self.total += value
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)

    def to_result(
        self,
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        parameter_id: int,
    ) -> AggregateResult:
        """Build an immutable aggregate result from current state.

        Args:
            start_ts_ns: Inclusive start timestamp of the aggregated range.
            end_ts_ns: Exclusive end timestamp of the aggregated range.
            parameter_id: Identifier of the parameter being aggregated.

        Returns:
            AggregateResult: Final aggregate values for this parameter.

        """
        if self.count == 0:
            return AggregateResult(
                start_ts_ns=start_ts_ns,
                end_ts_ns=end_ts_ns,
                parameter_id=parameter_id,
                count=0,
            )
        return AggregateResult(
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
            parameter_id=parameter_id,
            count=self.count,
            min=self.min_value,
            max=self.max_value,
            avg=self.total / self.count,
        )


def aggregate_points(
    points: Iterable[Point],
    *,
    start_ts_ns: int,
    end_ts_ns: int,
    parameter_ids: set[int] | None = None,
) -> list[AggregateResult]:
    """Aggregate points by parameter for a fixed time interval.

    Args:
        points: Sequence of points to aggregate.
        start_ts_ns: Inclusive start timestamp of the query window.
        end_ts_ns: Exclusive end timestamp of the query window.
        parameter_ids: Optional parameter filter. If provided, only these
            parameters are aggregated and included in the output.

    Returns:
        list[AggregateResult]: Sorted per-parameter aggregate results.

    """
    accumulators: dict[int, _AggregateAccumulator] = {}
    if parameter_ids is not None:
        accumulators = {
            parameter_id: _AggregateAccumulator()
            for parameter_id in sorted(parameter_ids)
        }

    for point in points:
        if parameter_ids is not None and point.parameter_id not in parameter_ids:
            continue
        accumulator = accumulators.setdefault(
            point.parameter_id,
            _AggregateAccumulator(),
        )
        accumulator.add(point.value)

    return [
        accumulators[parameter_id].to_result(
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
            parameter_id=parameter_id,
        )
        for parameter_id in sorted(accumulators)
    ]


def query_aggregates(
    storage: StorageCore,
    start_ts_ns: int,
    end_ts_ns: int,
    parameter_ids: set[int] | None = None,
) -> list[AggregateResult]:
    """Load points from storage and return aggregated results.

    Args:
        storage: Storage core used for range reads.
        start_ts_ns: Inclusive start timestamp of the query window.
        end_ts_ns: Exclusive end timestamp of the query window.
        parameter_ids: Optional parameter filter.

    Returns:
        list[AggregateResult]: Aggregate output for requested parameters.

    """
    points = storage.query_range(start_ts_ns, end_ts_ns, parameter_ids)
    return aggregate_points(
        points,
        start_ts_ns=start_ts_ns,
        end_ts_ns=end_ts_ns,
        parameter_ids=parameter_ids,
    )


def aggregate_results_to_rows(results: Iterable[AggregateResult]) -> list[AggregateRow]:
    """Convert aggregate objects to row dictionaries.

    Args:
        results: Aggregate results to serialize.

    Returns:
        list[AggregateRow]: List of dictionary rows.

    """
    return [result.to_row() for result in results]
