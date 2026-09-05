"""Flight telemetry plotting helpers for an interactive matplotlib window."""

from __future__ import annotations

from dataclasses import dataclass
import math
import random
from typing import TYPE_CHECKING, Any

from ..flt import FLTData
from .models import Point

if TYPE_CHECKING:
    from .core import StorageCore

DISPLAY_Y_MIN = 0.0
DISPLAY_Y_MAX = 10_000.0
ALTITUDE_DETAIL_RATIO = 0.1
DEFAULT_PARAMETER_NAMES = (
    "BaroAltitude",
    "RadioAltitude",
    "IAS",
    "TAS",
    "GroundSpeed",
    "VerticalSpeed",
    "PitchAngle",
    "RollAngle",
    "YawAngle",
    "Engine1_N1",
)
LIVE_DEMO_JITTER_INTERVAL_SECONDS = 6.0
LIVE_DEMO_JITTER_EDGE_TAPER_SECONDS = 8.0
LIVE_DEMO_TAKEOFF_SECONDS = 28.0


@dataclass(frozen=True, slots=True)
class PlotSpec:
    """Static description of one telemetry line rendered on the chart."""

    name: str
    color: str
    band_key: str


@dataclass(frozen=True, slots=True)
class PlotBand:
    """Display band and raw range shared by one or more plot series."""

    key: str
    label: str
    color: str
    display_min: float
    display_max: float
    raw_min: float
    raw_max: float
    unit: str


@dataclass(frozen=True, slots=True)
class PlotSeries:
    """Prepared plot series for one telemetry parameter."""

    name: str
    unit: str
    color: str
    seconds: list[float]
    raw_values: list[float]
    display_values: list[float]


@dataclass(frozen=True, slots=True)
class ScaleGuide:
    """One colored guide line rendered left of the main chart area."""

    title: str
    series_labels: tuple[str, ...]
    color: str
    display_min: float
    display_max: float
    raw_min: float
    raw_max: float
    unit: str
    tick_values: tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class FlightPlotData:
    """All derived data required to render the custom flight plot."""

    target_seconds: float
    x_max_seconds: float
    altitude_reference_ft: float
    altitude_detail_ft: float
    speed_min_kt: float
    speed_max_kt: float
    series: tuple[PlotSeries, ...]
    guides: tuple[ScaleGuide, ...]


DEFAULT_PLOT_SPECS = (
    PlotSpec(name="BaroAltitude", color="#D1495B", band_key="altitude"),
    PlotSpec(name="RadioAltitude", color="#2A9D8F", band_key="altitude"),
    PlotSpec(name="IAS", color="#1D4ED8", band_key="speed"),
    PlotSpec(name="TAS", color="#7C3AED", band_key="speed"),
    PlotSpec(name="GroundSpeed", color="#0F766E", band_key="speed"),
    PlotSpec(name="VerticalSpeed", color="#C2410C", band_key="vertical_speed"),
    PlotSpec(name="PitchAngle", color="#B91C1C", band_key="attitude"),
    PlotSpec(name="RollAngle", color="#15803D", band_key="attitude"),
    PlotSpec(name="YawAngle", color="#A16207", band_key="attitude"),
    PlotSpec(name="Engine1_N1", color="#4F46E5", band_key="engine_n1"),
)


def build_parameter_map(flt_data: FLTData) -> dict[str, int]:
    """Build mapping from parameter name to parameter id.

    Args:
        flt_data: Parsed FLT payload.

    Returns:
        Mapping keyed by FLT parameter name.
    """
    return {entry.name: entry.parameter_id for entry in flt_data.layout}


def generate_demo_flight_plot_points(
    flt_data: FLTData,
    *,
    target_seconds: float,
    samples_per_second: int = 4,
) -> list[Point]:
    """Generate linear synthetic telemetry points for visual plot debugging.

    Args:
        flt_data: Parsed FLT payload used for parameter ids.
        target_seconds: Requested x-axis horizon shown on the plot.
        samples_per_second: Number of samples emitted per second.

    Returns:
        Synthetic point stream covering the default 10 plotted parameters.

    Raises:
        ValueError: If ``target_seconds`` or ``samples_per_second`` is not positive.
    """
    if target_seconds <= 0:
        raise ValueError("target_seconds must be positive")
    if samples_per_second <= 0:
        raise ValueError("samples_per_second must be positive")

    parameter_map = build_parameter_map(flt_data)
    required_names = set(DEFAULT_PARAMETER_NAMES)
    if not required_names.issubset(parameter_map):
        missing = sorted(required_names - set(parameter_map))
        raise ValueError(f"FLT is missing required parameters: {', '.join(missing)}")

    steps = max(2, int(target_seconds * samples_per_second) + 1)
    step_ns = int(1_000_000_000 / samples_per_second)
    points: list[Point] = []

    demo_ranges = {
        "BaroAltitude": (0.0, 8_000.0),
        "RadioAltitude": (200.0, 7_200.0),
        "IAS": (120.0, 340.0),
        "TAS": (140.0, 370.0),
        "GroundSpeed": (135.0, 360.0),
        "VerticalSpeed": (-1_500.0, 2_500.0),
        "PitchAngle": (-4.0, 9.0),
        "RollAngle": (-20.0, 25.0),
        "YawAngle": (-8.0, 12.0),
        "Engine1_N1": (58.0, 96.0),
    }

    for index in range(steps):
        ratio = index / (steps - 1)
        timestamp_ns = index * step_ns
        for name in DEFAULT_PARAMETER_NAMES:
            raw_min, raw_max = demo_ranges[name]
            value = raw_min + ((raw_max - raw_min) * ratio)
            points.append(
                Point(
                    timestamp_ns=timestamp_ns,
                    parameter_id=parameter_map[name],
                    value=value,
                )
            )
    return points


def generate_live_demo_flight_plot_points(
    flt_data: FLTData,
    *,
    target_seconds: float = 120.0,
    samples_per_second: int = 4,
) -> list[Point]:
    """Generate a visually pleasant synthetic takeoff profile for demos.

    Args:
        flt_data: Parsed FLT payload used for parameter ids.
        target_seconds: Fixed flight horizon for the generated demo.
        samples_per_second: Number of samples emitted per second.

    Returns:
        Synthetic point stream representing a plausible 120-second takeoff.
    """
    if target_seconds <= 0:
        raise ValueError("target_seconds must be positive")
    if samples_per_second <= 0:
        raise ValueError("samples_per_second must be positive")

    parameter_map = build_parameter_map(flt_data)
    required_names = set(DEFAULT_PARAMETER_NAMES)
    if not required_names.issubset(parameter_map):
        missing = sorted(required_names - set(parameter_map))
        raise ValueError(f"FLT is missing required parameters: {', '.join(missing)}")

    steps = max(2, int(target_seconds * samples_per_second) + 1)
    step_ns = int(1_000_000_000 / samples_per_second)
    points: list[Point] = []

    for index in range(steps):
        seconds = index / samples_per_second
        ratio = min(1.0, seconds / max(target_seconds, 1.0))
        timestamp_ns = index * step_ns

        baro_altitude = _takeoff_altitude_profile(seconds)
        radio_altitude = max(0.0, baro_altitude - 120.0)
        ias = _smooth_step(seconds, 0.0, 60.0, 0.0, 165.0) + _smooth_step(
            seconds, 60.0, 120.0, 0.0, 20.0
        )
        tas = ias + _smooth_step(seconds, 20.0, 120.0, 8.0, 32.0)
        ground_speed = max(0.0, ias - _smooth_step(seconds, 0.0, 30.0, 20.0, 5.0))
        vertical_speed = _takeoff_vertical_speed_profile(seconds)
        pitch_angle = _piecewise_profile(
            seconds,
            (
                (0.0, 25.0, 0.0, 0.0),
                (25.0, 38.0, 0.0, 11.0),
                (38.0, 75.0, 11.0, 8.0),
                (75.0, 120.0, 8.0, 6.0),
            ),
        )
        roll_angle = _piecewise_profile(
            seconds,
            (
                (0.0, 55.0, 0.0, 0.0),
                (55.0, 72.0, 0.0, 14.0),
                (72.0, 88.0, 14.0, -10.0),
                (88.0, 120.0, -10.0, 0.0),
            ),
        )
        yaw_angle = _piecewise_profile(
            seconds,
            (
                (0.0, 28.0, 0.0, 0.0),
                (28.0, 40.0, 0.0, 4.0),
                (40.0, 90.0, 4.0, 1.5),
                (90.0, 120.0, 1.5, 0.0),
            ),
        )
        engine_n1 = _piecewise_profile(
            seconds,
            (
                (0.0, 10.0, 35.0, 55.0),
                (10.0, 26.0, 55.0, 96.0),
                (26.0, 60.0, 96.0, 94.0),
                (60.0, 120.0, 94.0, 88.0),
            ),
        )

        values_by_name = {
            "BaroAltitude": baro_altitude,
            "RadioAltitude": radio_altitude,
            "IAS": ias,
            "TAS": tas,
            "GroundSpeed": ground_speed,
            "VerticalSpeed": vertical_speed,
            "PitchAngle": pitch_angle,
            "RollAngle": roll_angle,
            "YawAngle": yaw_angle,
            "Engine1_N1": engine_n1,
        }
        _apply_live_demo_jitter(
            values_by_name,
            seconds=seconds,
            target_seconds=target_seconds,
        )

        for name in DEFAULT_PARAMETER_NAMES:
            points.append(
                Point(
                    timestamp_ns=timestamp_ns,
                    parameter_id=parameter_map[name],
                    value=values_by_name[name],
                )
            )

        if ratio >= 1.0:
            break

    return points


def _apply_live_demo_jitter(
    values_by_name: dict[str, float],
    *,
    seconds: float,
    target_seconds: float,
) -> None:
    """Add small smooth pseudo-random motion to the live demo traces."""
    amplitudes = {
        "BaroAltitude": 70.0,
        "RadioAltitude": 55.0,
        "IAS": 2.4,
        "TAS": 2.8,
        "GroundSpeed": 3.2,
        "VerticalSpeed": 120.0,
        "PitchAngle": 0.35,
        "RollAngle": 0.9,
        "YawAngle": 0.3,
        "Engine1_N1": 0.7,
    }
    clamps = {
        "BaroAltitude": (0.0, None),
        "RadioAltitude": (0.0, None),
        "IAS": (0.0, None),
        "TAS": (0.0, None),
        "GroundSpeed": (0.0, None),
        "Engine1_N1": (0.0, 100.0),
    }

    for name, amplitude in amplitudes.items():
        jitter = _sample_live_demo_jitter(
            name,
            seconds=seconds,
            target_seconds=target_seconds,
        )
        value = values_by_name[name] + (jitter * amplitude)
        min_value, max_value = clamps.get(name, (None, None))
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        values_by_name[name] = value


def _sample_live_demo_jitter(
    name: str,
    *,
    seconds: float,
    target_seconds: float,
) -> float:
    """Return deterministic smooth pseudo-random noise in the range [-1, 1]."""
    taper = _edge_taper(seconds, target_seconds=target_seconds)
    if taper <= 0.0:
        return 0.0

    anchor_position = seconds / LIVE_DEMO_JITTER_INTERVAL_SECONDS
    left_anchor = int(math.floor(anchor_position))
    right_anchor = left_anchor + 1
    blend = anchor_position - left_anchor
    eased_blend = blend * blend * (3.0 - (2.0 * blend))
    left_value = _live_demo_noise_anchor(name, left_anchor)
    right_value = _live_demo_noise_anchor(name, right_anchor)
    return (left_value + ((right_value - left_value) * eased_blend)) * taper


def _live_demo_noise_anchor(name: str, anchor_index: int) -> float:
    """Return one deterministic pseudo-random anchor value for a series."""
    seed = 17
    for char in name:
        seed = (seed * 131) + ord(char)
    rng = random.Random(seed + (anchor_index * 1_009))
    return rng.uniform(-1.0, 1.0)


def _edge_taper(seconds: float, *, target_seconds: float) -> float:
    """Fade jitter in and out so start and end values stay stable."""
    if target_seconds <= 0 or seconds <= LIVE_DEMO_TAKEOFF_SECONDS:
        return 0.0
    takeoff_seconds = min(LIVE_DEMO_TAKEOFF_SECONDS, target_seconds)
    fade_in = min(
        1.0,
        max(
            0.0,
            (seconds - takeoff_seconds) / LIVE_DEMO_JITTER_EDGE_TAPER_SECONDS,
        ),
    )
    fade_out = min(
        1.0,
        max(0.0, (target_seconds - seconds) / LIVE_DEMO_JITTER_EDGE_TAPER_SECONDS),
    )
    return min(fade_in, fade_out)


def prepare_flight_plot_data(
    points: list[Point],
    flt_data: FLTData,
    *,
    target_seconds: float,
) -> FlightPlotData:
    """Transform queried telemetry points into custom display coordinates.

    Args:
        points: Query result containing the parameters needed for plotting.
        flt_data: Parsed FLT metadata used for parameter ids and units.
        target_seconds: Requested flight time horizon shown on the x-axis.

    Returns:
        Fully prepared plot payload for window rendering.

    Raises:
        ValueError: If ``target_seconds`` is not positive or points are missing.
    """
    if target_seconds <= 0:
        raise ValueError("target_seconds must be positive")
    if not points:
        raise ValueError("No telemetry points found for plotting")

    layout_by_id = {entry.parameter_id: entry for entry in flt_data.layout}
    parameter_map = build_parameter_map(flt_data)
    required_names = set(DEFAULT_PARAMETER_NAMES)
    if not required_names.issubset(parameter_map):
        missing = sorted(required_names - set(parameter_map))
        raise ValueError(f"FLT is missing required parameters: {', '.join(missing)}")

    base_timestamp_ns = min(point.timestamp_ns for point in points)
    visible_points = [
        point
        for point in points
        if ((point.timestamp_ns - base_timestamp_ns) / 1_000_000_000) <= target_seconds
    ]
    if not visible_points:
        raise ValueError("No telemetry points fall into the requested target_seconds")

    points_by_parameter: dict[int, list[Point]] = {}
    for point in visible_points:
        points_by_parameter.setdefault(point.parameter_id, []).append(point)

    baro_points = points_by_parameter.get(parameter_map["BaroAltitude"], [])
    radio_points = points_by_parameter.get(parameter_map["RadioAltitude"], [])
    if not baro_points and not radio_points:
        raise ValueError("Altitude series are missing in the requested time range")

    altitude_reference = _resolve_altitude_reference(
        baro_points=baro_points,
        radio_points=radio_points,
        base_timestamp_ns=base_timestamp_ns,
        target_seconds=target_seconds,
    )
    altitude_detail = altitude_reference * ALTITUDE_DETAIL_RATIO
    bands = _build_bands(
        parameter_map=parameter_map,
        points_by_parameter=points_by_parameter,
        altitude_reference=altitude_reference,
        altitude_detail=altitude_detail,
    )

    series: list[PlotSeries] = []
    for spec in DEFAULT_PLOT_SPECS:
        parameter_id = parameter_map[spec.name]
        series_points = points_by_parameter.get(parameter_id, [])
        if not series_points:
            continue
        band = bands[spec.band_key]
        series.append(
            _build_series(
                name=spec.name,
                color=spec.color,
                unit=layout_by_id[parameter_id].unit,
                points=series_points,
                base_timestamp_ns=base_timestamp_ns,
                raw_min=band.raw_min,
                raw_max=band.raw_max,
                display_min=band.display_min,
                display_max=band.display_max,
            )
        )

    return FlightPlotData(
        target_seconds=target_seconds,
        x_max_seconds=target_seconds,
        altitude_reference_ft=altitude_reference,
        altitude_detail_ft=altitude_detail,
        speed_min_kt=bands["speed"].raw_min,
        speed_max_kt=bands["speed"].raw_max,
        series=tuple(series),
        guides=tuple(
            ScaleGuide(
                title=band.label,
                series_labels=tuple(
                    spec.name
                    for spec in DEFAULT_PLOT_SPECS
                    if spec.band_key == band_key
                    and points_by_parameter.get(parameter_map[spec.name], [])
                ),
                color=band.color,
                display_min=band.display_min,
                display_max=band.display_max,
                raw_min=band.raw_min,
                raw_max=band.raw_max,
                unit=band.unit,
                tick_values=_build_guide_tick_values(band.raw_min, band.raw_max),
            )
            for band_key, band in bands.items()
            if band_key != "altitude"
        ),
    )


def show_flight_plot_window(
    storage: StorageCore,
    flt_data: FLTData,
    *,
    target_seconds: float,
) -> None:
    """Open an interactive matplotlib window with the custom flight plot.

    Args:
        storage: Storage queried for telemetry samples.
        flt_data: Parsed FLT metadata used for parameter lookup.
        target_seconds: Requested x-axis horizon in seconds.
    """
    parameter_map = build_parameter_map(flt_data)
    parameter_ids = {
        parameter_map[name] for name in DEFAULT_PARAMETER_NAMES if name in parameter_map
    }
    points = storage.query_range(0, 2**63 - 1, parameter_ids)
    show_flight_plot_points_window(
        points,
        flt_data,
        target_seconds=target_seconds,
        title_prefix="Flight Telemetry View (10 parameters)",
    )


def show_flight_plot_points_window(
    points: list[Point],
    flt_data: FLTData,
    *,
    target_seconds: float,
    title_prefix: str,
) -> None:
    """Open an interactive matplotlib window from an explicit point list.

    Args:
        points: Telemetry points to render.
        flt_data: Parsed FLT metadata used for parameter lookup.
        target_seconds: Requested x-axis horizon in seconds.
        title_prefix: Title prefix shown at the top of the plot window.
    """
    plot_data = prepare_flight_plot_data(
        points,
        flt_data,
        target_seconds=target_seconds,
    )

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(15, 8.5), dpi=140)
    left_margin = max(4.0, plot_data.x_max_seconds * 0.18)
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#FCFCFD")

    ax.set_xlim(-left_margin, plot_data.x_max_seconds)
    ax.set_ylim(DISPLAY_Y_MIN, DISPLAY_Y_MAX)
    y_grid_ticks = [
        DISPLAY_Y_MIN + ((DISPLAY_Y_MAX - DISPLAY_Y_MIN) * step / 30)
        for step in range(31)
    ]
    ax.set_yticks(y_grid_ticks)
    for spine in ax.spines.values():
        spine.set_color("#334155")
        spine.set_linewidth(1.1)
        spine.set_antialiased(False)

    ax.grid(
        axis="y",
        color="#94A3B8",
        linestyle=(0, (2, 2)),
        linewidth=0.55,
        alpha=0.55,
        antialiased=False,
    )
    ax.tick_params(axis="x", colors="#0F172A", width=0.9, length=4)
    ax.tick_params(axis="y", length=0, labelleft=False)

    for series in plot_data.series:
        ax.plot(
            series.seconds,
            series.display_values,
            color=series.color,
            linewidth=1.15,
            antialiased=False,
            solid_capstyle="butt",
            solid_joinstyle="miter",
            label=f"{series.name} [{series.unit}]",
        )

    _draw_guides(ax, plot_data, left_margin)

    ax.set_xlabel("Flight time, s")
    ax.set_ylabel("Display units")
    ax.set_title(
        f"{title_prefix}\n"
        f"target_seconds={plot_data.target_seconds:g}, "
        f"altitude_ref={plot_data.altitude_reference_ft:.1f} ft, "
        f"speed range={plot_data.speed_min_kt:.1f}..{plot_data.speed_max_kt:.1f} kt"
    )
    fig.tight_layout()
    plt.show()


def _draw_guides(ax: Any, plot_data: FlightPlotData, left_margin: float) -> None:
    """Render colored scale guides in the reserved left-side gutter."""
    guide_count = len(plot_data.guides)
    if guide_count == 0:
        return

    if guide_count == 1:
        x_positions = [-left_margin * 0.16]
    else:
        x_positions = [
            -left_margin * (0.2 - (0.08 * index / (guide_count - 1)))
            for index in range(guide_count)
        ]

    for x_position, guide in zip(x_positions, plot_data.guides, strict=True):
        ax.vlines(
            x_position,
            guide.display_min,
            guide.display_max,
            colors=guide.color,
            linewidth=3.0,
        )
        guide_series_text = _build_guide_series_text(guide.series_labels)
        if guide_series_text:
            label_anchor_y = guide.display_max - 60.0
            ax.text(
                x_position - (left_margin * 0.03),
                max(label_anchor_y, guide.display_min + 20.0),
                guide_series_text,
                color=guide.color,
                ha="right",
                va="top",
                fontsize=8,
                linespacing=1.1,
                clip_on=False,
            )
        tick_half_width = left_margin * 0.025
        tick_label_x = x_position - (left_margin * 0.03)
        for tick_fraction, tick_value in guide.tick_values:
            tick_display_y = _scale_guide_fraction(
                tick_fraction,
                display_min=guide.display_min,
                display_max=guide.display_max,
            )
            ax.hlines(
                tick_display_y,
                x_position - tick_half_width,
                x_position + tick_half_width,
                colors=guide.color,
                linewidth=2.0,
            )
            ax.text(
                tick_label_x,
                tick_display_y,
                _format_guide_tick_value(tick_value),
                color=guide.color,
                ha="right",
                va="center",
                fontsize=8,
                clip_on=False,
            )


def _build_bands(
    *,
    parameter_map: dict[str, int],
    points_by_parameter: dict[int, list[Point]],
    altitude_reference: float,
    altitude_detail: float,
) -> dict[str, PlotBand]:
    """Create display bands for altitude, speed, attitude, and engine groups."""
    speed_min, speed_max = _resolve_range(
        _collect_values(
            ("IAS", "TAS", "GroundSpeed"),
            parameter_map=parameter_map,
            points_by_parameter=points_by_parameter,
        ),
        fallback_min=0.0,
        fallback_max=500.0,
    )
    vertical_speed_min, vertical_speed_max = _resolve_range(
        _collect_values(
            ("VerticalSpeed",),
            parameter_map=parameter_map,
            points_by_parameter=points_by_parameter,
        ),
        fallback_min=-6_000.0,
        fallback_max=6_000.0,
    )
    attitude_min, attitude_max = _resolve_range(
        _collect_values(
            ("PitchAngle", "RollAngle", "YawAngle"),
            parameter_map=parameter_map,
            points_by_parameter=points_by_parameter,
        ),
        fallback_min=-180.0,
        fallback_max=180.0,
    )
    engine_min, engine_max = _resolve_range(
        _collect_values(
            ("Engine1_N1",),
            parameter_map=parameter_map,
            points_by_parameter=points_by_parameter,
        ),
        fallback_min=0.0,
        fallback_max=110.0,
    )

    return {
        "altitude": PlotBand(
            key="altitude",
            label="Altitude full",
            color="#D1495B",
            display_min=DISPLAY_Y_MIN,
            display_max=DISPLAY_Y_MAX,
            raw_min=0.0,
            raw_max=altitude_reference,
            unit="ft",
        ),
        "altitude_detail": PlotBand(
            key="altitude_detail",
            label="Altitude",
            color="#2A9D8F",
            display_min=DISPLAY_Y_MIN,
            display_max=DISPLAY_Y_MAX * ALTITUDE_DETAIL_RATIO,
            raw_min=0.0,
            raw_max=altitude_detail,
            unit="ft",
        ),
        "speed": PlotBand(
            key="speed",
            label="Speed",
            color="#1D4ED8",
            display_min=1_300.0,
            display_max=3_000.0,
            raw_min=speed_min,
            raw_max=speed_max,
            unit="kt",
        ),
        "vertical_speed": PlotBand(
            key="vertical_speed",
            label="VSpeed",
            color="#C2410C",
            display_min=3_900.0,
            display_max=5_500.0,
            raw_min=vertical_speed_min,
            raw_max=vertical_speed_max,
            unit="ft/min",
        ),
        "attitude": PlotBand(
            key="attitude",
            label="Attitude",
            color="#B91C1C",
            display_min=6_400.0,
            display_max=7_900.0,
            raw_min=attitude_min,
            raw_max=attitude_max,
            unit="deg",
        ),
        "engine_n1": PlotBand(
            key="engine_n1",
            label="Engine N1",
            color="#4F46E5",
            display_min=8_500.0,
            display_max=9_700.0,
            raw_min=engine_min,
            raw_max=engine_max,
            unit="%",
        ),
    }


def _build_guide_tick_values(
    raw_min: float, raw_max: float
) -> tuple[tuple[float, float], ...]:
    """Build real tick labels for the lower 0% and 50% of one scale guide."""
    tick_values: list[tuple[float, float]] = []
    for fraction in (0.5, 0.0):
        value = raw_min + ((raw_max - raw_min) * fraction)
        if any(
            math.isclose(value, existing_value, rel_tol=1e-9, abs_tol=1e-9)
            for _, existing_value in tick_values
        ):
            continue
        tick_values.append((fraction, value))
    return tuple(tick_values)


def _scale_guide_fraction(
    fraction: float, *, display_min: float, display_max: float
) -> float:
    """Scale a normalized guide fraction into display coordinates."""
    return display_min + ((display_max - display_min) * fraction)


def _format_guide_tick_value(value: float) -> str:
    """Format guide tick labels without trailing decimal noise."""
    rounded_value = round(value)
    if math.isclose(value, rounded_value, rel_tol=1e-9, abs_tol=1e-9):
        return str(int(rounded_value))
    return f"{value:.1f}"


def _build_guide_series_text(series_labels: tuple[str, ...]) -> str:
    """Build a multi-line series list shown above one scale guide."""
    return "\n".join(_format_guide_series_label(label) for label in series_labels)


def _format_guide_series_label(label: str) -> str:
    """Convert internal telemetry names to compact labels for guide headers."""
    replacements = {
        "GroundSpeed": "Ground speed",
        "VerticalSpeed": "Vertical speed",
        "Engine1_N1": "Engine1 N1",
    }
    return replacements.get(label, label)


def _collect_values(
    names: tuple[str, ...],
    *,
    parameter_map: dict[str, int],
    points_by_parameter: dict[int, list[Point]],
) -> list[float]:
    """Collect raw telemetry values for one grouped display band."""
    values: list[float] = []
    for name in names:
        parameter_id = parameter_map.get(name)
        if parameter_id is None:
            continue
        values.extend(
            point.value for point in points_by_parameter.get(parameter_id, [])
        )
    return values


def _resolve_range(
    values: list[float],
    *,
    fallback_min: float,
    fallback_max: float,
) -> tuple[float, float]:
    """Return a stable min/max pair for one display band."""
    if not values:
        return (fallback_min, fallback_max)
    raw_min = min(values)
    raw_max = max(values)
    if raw_max <= raw_min:
        raw_max = raw_min + 1.0
    return (raw_min, raw_max)


def _build_series(
    *,
    name: str,
    color: str,
    unit: str,
    points: list[Point],
    base_timestamp_ns: int,
    raw_min: float,
    raw_max: float,
    display_min: float,
    display_max: float,
) -> PlotSeries:
    """Convert raw points into x/y arrays using one display band."""
    seconds = [
        (point.timestamp_ns - base_timestamp_ns) / 1_000_000_000 for point in points
    ]
    raw_values = [point.value for point in points]
    display_values = [
        _scale_value(
            point.value,
            raw_min=raw_min,
            raw_max=raw_max,
            display_min=display_min,
            display_max=display_max,
        )
        for point in points
    ]
    return PlotSeries(
        name=name,
        unit=unit,
        color=color,
        seconds=seconds,
        raw_values=raw_values,
        display_values=display_values,
    )


def _resolve_altitude_reference(
    *,
    baro_points: list[Point],
    radio_points: list[Point],
    base_timestamp_ns: int,
    target_seconds: float,
) -> float:
    """Choose the altitude reference used to scale the full-height altitude band."""
    reference = _value_at_or_before(
        baro_points,
        base_timestamp_ns=base_timestamp_ns,
        target_seconds=target_seconds,
    )
    if reference is None:
        reference = _value_at_or_before(
            radio_points,
            base_timestamp_ns=base_timestamp_ns,
            target_seconds=target_seconds,
        )
    if reference is None or reference <= 0:
        fallback_values = [point.value for point in [*baro_points, *radio_points]]
        reference = max(fallback_values, default=1.0)
    return max(reference, 1.0)


def _value_at_or_before(
    points: list[Point],
    *,
    base_timestamp_ns: int,
    target_seconds: float,
) -> float | None:
    """Return the most recent value at or before ``target_seconds``."""
    latest_value: float | None = None
    for point in points:
        seconds = (point.timestamp_ns - base_timestamp_ns) / 1_000_000_000
        if seconds <= target_seconds:
            latest_value = point.value
        else:
            break
    return latest_value


def _scale_value(
    value: float,
    *,
    raw_min: float,
    raw_max: float,
    display_min: float,
    display_max: float,
) -> float:
    """Scale one raw value into the requested display band."""
    if raw_max <= raw_min:
        return (display_min + display_max) / 2.0
    ratio = (value - raw_min) / (raw_max - raw_min)
    clamped_ratio = min(max(ratio, 0.0), 1.0)
    return display_min + ((display_max - display_min) * clamped_ratio)


def _takeoff_altitude_profile(seconds: float) -> float:
    """Return a smooth climb profile in feet for the demo takeoff."""
    return _piecewise_profile(
        seconds,
        (
            (0.0, 28.0, 0.0, 0.0),
            (28.0, 45.0, 0.0, 900.0),
            (45.0, 80.0, 900.0, 3_800.0),
            (80.0, 120.0, 3_800.0, 9_200.0),
        ),
    )


def _takeoff_vertical_speed_profile(seconds: float) -> float:
    """Return a smooth vertical speed profile in ft/min for the demo takeoff."""
    return _piecewise_profile(
        seconds,
        (
            (0.0, 28.0, 0.0, 0.0),
            (28.0, 40.0, 0.0, 2_600.0),
            (40.0, 75.0, 2_600.0, 3_400.0),
            (75.0, 120.0, 3_400.0, 2_100.0),
        ),
    )


def _piecewise_profile(
    seconds: float,
    segments: tuple[tuple[float, float, float, float], ...],
) -> float:
    """Evaluate a piecewise-smooth linear profile."""
    for start_s, end_s, start_v, end_v in segments:
        if seconds <= end_s:
            return _smooth_step(seconds, start_s, end_s, start_v, end_v)
    return segments[-1][3]


def _smooth_step(
    seconds: float,
    start_s: float,
    end_s: float,
    start_v: float,
    end_v: float,
) -> float:
    """Interpolate smoothly between two values across a time interval."""
    if end_s <= start_s:
        return end_v
    if seconds <= start_s:
        return start_v
    if seconds >= end_s:
        return end_v
    ratio = (seconds - start_s) / (end_s - start_s)
    eased = ratio * ratio * (3.0 - (2.0 * ratio))
    return start_v + ((end_v - start_v) * eased)
