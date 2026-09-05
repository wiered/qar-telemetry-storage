"""Tests for interactive flight plotting helpers."""

from core.flt import FLTParser
from core.storage import Point
from core.storage.flight_plot import (
    DEFAULT_PARAMETER_NAMES,
    generate_demo_flight_plot_points,
    generate_live_demo_flight_plot_points,
    prepare_flight_plot_data,
)


def make_points() -> list[Point]:
    """Build a small deterministic point set covering all default plot parameters."""
    return [
        Point(timestamp_ns=0, parameter_id=1, value=200.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=1, value=220.0),
        Point(timestamp_ns=0, parameter_id=2, value=210.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=2, value=225.0),
        Point(timestamp_ns=0, parameter_id=4, value=0.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=4, value=5_000.0),
        Point(timestamp_ns=0, parameter_id=5, value=50.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=5, value=4_800.0),
        Point(timestamp_ns=0, parameter_id=6, value=-500.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=6, value=1_500.0),
        Point(timestamp_ns=0, parameter_id=7, value=-3.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=7, value=5.0),
        Point(timestamp_ns=0, parameter_id=8, value=-10.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=8, value=15.0),
        Point(timestamp_ns=0, parameter_id=9, value=2.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=9, value=8.0),
        Point(timestamp_ns=0, parameter_id=10, value=70.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=10, value=92.0),
        Point(timestamp_ns=0, parameter_id=20, value=205.0),
        Point(timestamp_ns=1_000_000_000, parameter_id=20, value=230.0),
    ]


def test_prepare_flight_plot_data_builds_ten_series():
    """Default telemetry plot includes ten named series with shared display bands."""
    flt_data = FLTParser("data/base_flt.json").data

    plot_data = prepare_flight_plot_data(
        make_points(),
        flt_data,
        target_seconds=1.0,
    )

    assert len(plot_data.series) == 10
    assert len(plot_data.guides) == 5
    assert any(guide.title == "Altitude" for guide in plot_data.guides)
    assert all(guide.title != "Altitude full" for guide in plot_data.guides)
    assert plot_data.altitude_reference_ft == 5_000.0
    assert plot_data.altitude_detail_ft == 500.0
    assert plot_data.speed_min_kt == 200.0
    assert plot_data.speed_max_kt == 230.0


def test_prepare_flight_plot_data_scales_altitude_and_speed_bands():
    """Altitude occupies the full height while speed occupies the dedicated band."""
    flt_data = FLTParser("data/base_flt.json").data

    plot_data = prepare_flight_plot_data(
        make_points(),
        flt_data,
        target_seconds=1.0,
    )

    series_by_name = {series.name: series for series in plot_data.series}
    assert series_by_name["BaroAltitude"].display_values == [0.0, 10_000.0]
    assert series_by_name["IAS"].display_values == [1_300.0, 2_433.333333333333]
    assert series_by_name["Engine1_N1"].display_values == [8_500.0, 9_700.0]


def test_prepare_flight_plot_data_builds_stacked_speed_guide_labels():
    """Shared guides expose a title, stacked series labels, and key ticks."""
    flt_data = FLTParser("data/base_flt.json").data

    plot_data = prepare_flight_plot_data(
        make_points(),
        flt_data,
        target_seconds=1.0,
    )

    speed_guide = next(guide for guide in plot_data.guides if guide.title == "Speed")
    assert speed_guide.series_labels == ("IAS", "TAS", "GroundSpeed")
    assert speed_guide.tick_values == ((0.5, 215.0), (0.0, 200.0))


def test_format_guide_series_label_humanizes_compound_names():
    """Guide header labels should be readable for compound telemetry names."""
    from core.storage.flight_plot import _format_guide_series_label

    assert _format_guide_series_label("GroundSpeed") == "Ground speed"
    assert _format_guide_series_label("IAS") == "IAS"


def test_build_guide_series_text_returns_multiline_series_names_only():
    """Guide caption renders parameter names without the generic band title."""
    from core.storage.flight_plot import _build_guide_series_text

    assert (
        _build_guide_series_text(("IAS", "GroundSpeed", "VerticalSpeed"))
        == "IAS\nGround speed\nVertical speed"
    )


def test_generate_demo_flight_plot_points_returns_linear_series_for_all_defaults():
    """Synthetic debug generator emits one linear series for each plotted parameter."""
    flt_data = FLTParser("data/base_flt.json").data

    points = generate_demo_flight_plot_points(
        flt_data,
        target_seconds=2.0,
        samples_per_second=2,
    )

    assert len(points) == 5 * len(DEFAULT_PARAMETER_NAMES)
    parameter_ids = {entry.parameter_id for entry in flt_data.layout}
    assert all(point.parameter_id in parameter_ids for point in points)
    assert points[0].timestamp_ns == 0
    assert points[-1].timestamp_ns == 2_000_000_000


def test_generate_live_demo_flight_plot_points_returns_takeoff_profile():
    """Presentation demo generator emits a climbing 120-second takeoff profile."""
    flt_data = FLTParser("data/base_flt.json").data

    points = generate_live_demo_flight_plot_points(
        flt_data,
        samples_per_second=2,
    )

    assert len(points) == 241 * len(DEFAULT_PARAMETER_NAMES)
    series_by_pid: dict[int, list[Point]] = {}
    for point in points:
        series_by_pid.setdefault(point.parameter_id, []).append(point)

    baro_points = series_by_pid[4]
    ias_points = series_by_pid[1]
    engine_points = series_by_pid[10]

    assert baro_points[0].value == 0.0
    assert baro_points[-1].value == 9_200.0
    assert ias_points[0].value == 0.0
    assert ias_points[-1].value > 180.0
    assert engine_points[0].value == 35.0
    assert engine_points[-1].value == 88.0
    assert all(point.value == 0.0 for point in baro_points[1:40])
    assert any(point.value != 0.0 for point in baro_points[60:100])
