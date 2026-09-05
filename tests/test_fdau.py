"""Tests for FDAUUnit (flight data acquisition unit) simulation."""

import dataclasses
import queue
import time

import pytest

import core.fdau as fdau_module
from core.fdau import FDAUUnit
from core.flt import FLTData, FLTLayout


@pytest.fixture
def simple_flt():
    """Minimal FLT layout for FDAU tests."""
    return FLTData(
        major_frame_sec=1,
        minor_frames=4,
        description="Test FLT",
        layout=[
            FLTLayout(
                name="IAS",
                description="",
                parameter_id=1,
                unit="kt",
                word=1,
                minor_frames=[1, 2, 3, 4],
                hz=4,
                type="float",
            ),
            FLTLayout(
                name="TAS",
                description="",
                parameter_id=2,
                unit="kt",
                word=2,
                minor_frames=[1, 3],
                hz=2,
                type="float",
            ),
            FLTLayout(
                name="Gear",
                description="",
                parameter_id=3,
                unit="-",
                word=3,
                minor_frames=[4],
                hz=1,
                type="discrete",
            ),
        ],
    )


def run_fdau_deterministically(
    monkeypatch, flt, *, hz=8, frames_to_collect=5, stress_mode=False, seed=1
):
    """Run FDAU with a deterministic fake clock; stop after ``frames_to_collect`` frames."""
    monkeypatch.setattr("settings.settings.hz", hz)

    clock = {
        "perf": 0.0,
        "wall": 1_700_000_000.0,
    }
    frames = []

    def fake_perf_counter():
        return clock["perf"]

    def fake_time():
        return clock["wall"] + clock["perf"]

    def fake_sleep(delay):
        clock["perf"] += delay

    monkeypatch.setattr(fdau_module.time, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(fdau_module.time, "time", fake_time)
    monkeypatch.setattr(fdau_module.time, "sleep", fake_sleep)

    fdau = FDAUUnit(flt, seed=seed, stress_mode=stress_mode)

    def on_frame(frame):
        frames.append(frame)
        if len(frames) >= frames_to_collect:
            fdau.stop()

    fdau.on_frame = on_frame
    fdau.run()
    return frames


def test_fdau_builds_expected_tick_map(monkeypatch, simple_flt):
    """Parameter tick sets match Hz and minor-frame schedule."""
    monkeypatch.setattr("settings.settings.hz", 8)
    fdau = FDAUUnit(simple_flt, seed=1)

    assert fdau._param_ticks["IAS"] == {1, 3, 5, 7}
    assert fdau._param_ticks["TAS"] == {1, 5}
    assert fdau._param_ticks["Gear"] == {7}


def test_fdau_emits_frames_and_values(monkeypatch, simple_flt):
    """Started FDAU delivers frames with seq, timestamps, and parameter values."""
    monkeypatch.setattr("settings.settings.hz", 8)
    frames = queue.Queue()

    fdau = FDAUUnit(simple_flt, on_frame=frames.put_nowait, seed=1)
    fdau.start()
    time.sleep(1.1)
    fdau.stop()
    fdau.join(timeout=2)

    collected = []
    while not frames.empty():
        collected.append(frames.get_nowait())

    assert collected
    assert all("seq" in frame for frame in collected)
    assert all("ts" in frame for frame in collected)
    assert all("ts_monotonic" in frame for frame in collected)
    assert all("values" in frame for frame in collected)
    assert any(frame["values"] for frame in collected)
    assert any("Gear" in frame["values"] for frame in collected)


def test_fdau_rejects_invalid_hz(monkeypatch, simple_flt):
    """Zero ``settings.hz`` raises before run."""
    monkeypatch.setattr("settings.settings.hz", 0)

    with pytest.raises(ValueError, match="settings.hz must be > 0"):
        FDAUUnit(simple_flt)


def test_fdau_rejects_invalid_flt_minor_frames(monkeypatch, simple_flt):
    """Zero ``flt.minor_frames`` raises."""
    monkeypatch.setattr("settings.settings.hz", 8)
    bad_flt = dataclasses.replace(simple_flt, minor_frames=0)

    with pytest.raises(ValueError, match="flt.minor_frames must be > 0"):
        FDAUUnit(bad_flt)


def test_fdau_rejects_minor_frame_below_range(monkeypatch, simple_flt):
    """Minor frame index below 1 raises."""
    monkeypatch.setattr("settings.settings.hz", 8)
    bad_layout = [
        dataclasses.replace(simple_flt.layout[0], minor_frames=[0]),
        *simple_flt.layout[1:],
    ]
    bad_flt = dataclasses.replace(simple_flt, layout=bad_layout)

    with pytest.raises(ValueError, match="minor_frame=0"):
        FDAUUnit(bad_flt)


def test_fdau_rejects_minor_frame_above_range(monkeypatch, simple_flt):
    """Minor frame index above ``flt.minor_frames`` raises."""
    monkeypatch.setattr("settings.settings.hz", 8)
    bad_layout = [
        dataclasses.replace(
            simple_flt.layout[0], minor_frames=[simple_flt.minor_frames + 1]
        ),
        *simple_flt.layout[1:],
    ]
    bad_flt = dataclasses.replace(simple_flt, layout=bad_layout)

    with pytest.raises(ValueError, match=f"minor_frame={simple_flt.minor_frames + 1}"):
        FDAUUnit(bad_flt)


def test_fdau_build_tick_map_collapses_duplicate_minor_frames(monkeypatch, simple_flt):
    """Duplicate minor-frame entries in layout collapse to unique ticks."""
    monkeypatch.setattr("settings.settings.hz", 8)
    dedup_param = dataclasses.replace(simple_flt.layout[0], minor_frames=[1, 1, 4, 4])
    flt = dataclasses.replace(simple_flt, layout=[dedup_param, *simple_flt.layout[1:]])

    fdau = FDAUUnit(flt, seed=1)

    assert fdau._param_ticks["IAS"] == {1, 7}


def test_fdau_gen_value_returns_expected_python_types(monkeypatch, simple_flt):
    """``_gen_value`` maps FLT types to float, int, discrete, or None."""
    monkeypatch.setattr("settings.settings.hz", 8)
    fdau = FDAUUnit(simple_flt, seed=1)

    assert isinstance(fdau._gen_value(simple_flt.layout[0]), float)
    assert isinstance(
        fdau._gen_value(dataclasses.replace(simple_flt.layout[0], type="int")), int
    )
    assert fdau._gen_value(
        dataclasses.replace(simple_flt.layout[0], type="discrete")
    ) in {0, 1}
    assert (
        fdau._gen_value(dataclasses.replace(simple_flt.layout[0], type="unknown"))
        is None
    )


def test_fdau_stress_mode_emits_all_parameters_on_every_tick(monkeypatch, simple_flt):
    """Stress mode includes every parameter on each emitted frame."""
    frames = run_fdau_deterministically(
        monkeypatch,
        simple_flt,
        hz=8,
        frames_to_collect=4,
        stress_mode=True,
        seed=1,
    )

    expected_names = {param.name for param in simple_flt.layout}

    assert frames
    assert all(set(frame["values"]) == expected_names for frame in frames)


def test_fdau_emitted_sequence_is_contiguous_and_ticks_stay_in_range(
    monkeypatch, simple_flt
):
    """Frame sequence numbers are contiguous; tick indices stay within Hz."""
    frames = run_fdau_deterministically(
        monkeypatch,
        simple_flt,
        hz=8,
        frames_to_collect=6,
        stress_mode=False,
        seed=1,
    )

    assert [frame["seq"] for frame in frames] == list(range(len(frames)))
    assert all(0 <= frame["tick"] < 8 for frame in frames)


def test_fdau_major_frame_advances_over_time(monkeypatch, simple_flt):
    """Major frame counter increases as simulated time advances."""
    flt = dataclasses.replace(simple_flt, major_frame_sec=0.5)
    frames = run_fdau_deterministically(
        monkeypatch,
        flt,
        hz=4,
        frames_to_collect=6,
        stress_mode=False,
        seed=1,
    )

    major_frames = [frame["major_frame"] for frame in frames]

    assert major_frames == sorted(major_frames)
    assert max(major_frames) >= 1


def test_fdau_rejects_non_positive_major_frame_length_on_run(monkeypatch, simple_flt):
    """``run()`` raises when ``flt.major_frame_sec`` is not positive."""
    monkeypatch.setattr("settings.settings.hz", 8)
    fdau = FDAUUnit(dataclasses.replace(simple_flt, major_frame_sec=0), seed=1)

    with pytest.raises(ValueError, match="flt.major_frame_sec must be > 0"):
        fdau.run()


def test_fdau_stop_before_start_is_safe(monkeypatch, simple_flt):
    """Calling ``stop()`` before ``start()`` leaves stop event set without error."""
    monkeypatch.setattr("settings.settings.hz", 8)
    fdau = FDAUUnit(simple_flt, seed=1)

    fdau.stop()

    assert fdau._stop_event.is_set()
