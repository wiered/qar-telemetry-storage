"""FDAU emulator thread that generates FLT-scheduled frames for testing."""

import threading
import time
import random
from typing import Callable, Optional, Dict, Any, TypedDict
from logging import getLogger

from settings import settings  # expects: settings.hz (int)
from core.flt import FLTData, FLTLayout, FLTParser

logger = getLogger(__name__)


class FDAUValue(TypedDict):
    """Payload for a single parameter value in a frame."""

    parameter_id: int
    value: Any


class FDAUFrame(TypedDict):
    """FDAU output frame payload."""

    seq: int
    ts: float
    ts_monotonic: float
    major_frame: int
    tick: int
    values: Dict[str, FDAUValue]


class FDAUUnit(threading.Thread):
    """FDAU emulator that emits FLT-based frames in a background thread.

    The unit runs at ``settings.hz`` ticks per second. Parameter values are
    generated only on ticks mapped from their FLT minor-frame schedule, unless
    ``stress_mode`` is enabled.

    Frame format:
        ``FDAUFrame`` with fields:
        ``seq``, ``ts``, ``ts_monotonic``, ``major_frame``, ``tick``,
        ``values``.

    Args:
        flt: FLT dataset used to generate output frames.
        on_frame: Optional callback receiving ``FDAUFrame`` for every generated
            frame.
        seed: Random seed for reproducible values; ``None`` uses default RNG
            state.
        stress_mode: If ``True``, emits all parameters on each tick.
    """

    def __init__(
        self,
        flt: FLTData,
        on_frame: Optional[Callable[[FDAUFrame], None]] = None,
        *,
        seed: Optional[int] = None,
        stress_mode: bool = False,
    ):
        super().__init__(daemon=True)
        self.flt = flt
        self.on_frame = on_frame
        self._stop_event = threading.Event()
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._stress_mode = stress_mode

        if seed is not None:
            random.seed(seed)
            logger.debug(f"Random seed set to {seed}")

        self._param_ticks: dict[str, set[int]] = self._build_param_tick_map()
        mode_str = "STRESS" if stress_mode else "normal"
        logger.info(
            f"FDAUUnit initialized with {len(self._param_ticks)} parameters, hz={settings.hz}, mode={mode_str}"
        )

    def _build_param_tick_map(self) -> dict[str, set[int]]:
        """Build mapping ``parameter name -> runtime ticks`` for one major frame.

        Returns:
            dict[str, set[int]]: Mapping where each key is a parameter name and
            each value is a set of tick indices in ``[0, settings.hz - 1]``.

        Raises:
            ValueError: If ``settings.hz <= 0``, ``flt.minor_frames <= 0``,
                or a parameter references an out-of-range minor frame.
        """
        hz = int(settings.hz)
        if hz <= 0:
            logger.error("settings.hz must be > 0")
            raise ValueError("settings.hz must be > 0")

        if self.flt.minor_frames <= 0:
            logger.error("flt.minor_frames must be > 0")
            raise ValueError("flt.minor_frames must be > 0")

        mapping: dict[str, set[int]] = {}

        for p in self.flt.layout:
            ticks: set[int] = set()
            for mf in p.minor_frames:
                if mf < 1 or mf > self.flt.minor_frames:
                    error_msg = (
                        f"Parameter '{p.name}' has minor_frame={mf}, "
                        f"but FLT minor_frames={self.flt.minor_frames}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                t = (mf - 0.5) / self.flt.minor_frames
                tick = int(t * hz)
                if tick >= hz:
                    tick = hz - 1
                ticks.add(tick)

            mapping[p.name] = ticks

        return mapping

    def stop(self) -> None:
        """Request background thread termination."""
        logger.info("Stopping FDAUUnit")
        self._stop_event.set()

    def _gen_value(self, p: FLTLayout) -> Any:
        """Generate a synthetic value for a parameter based on its type.

        Args:
            p: FLT layout entry for a parameter.

        Returns:
            Any: Generated value matching parameter type, or ``None`` for
            unknown parameter types.
        """
        t = p.type.lower()
        if t == "float":
            return round(random.uniform(0.0, 1_000.0), 3)
        if t == "int":
            return random.randint(0, 10_000)
        if t == "discrete":
            return random.randint(0, 1)
        return None

    def run(self) -> None:
        """Run main generation loop until stop signal is set.

        Raises:
            ValueError: If ``flt.major_frame_sec <= 0``.
        """
        logger.info("FDAUUnit thread started")
        hz = int(settings.hz)
        period = 1.0 / hz

        major_frame_len = float(self.flt.major_frame_sec)
        if major_frame_len <= 0:
            logger.error("flt.major_frame_sec must be > 0")
            raise ValueError("flt.major_frame_sec must be > 0")

        t0 = time.perf_counter()
        next_tick_time = t0
        logger.debug(
            f"Starting FDAUUnit run loop: hz={hz}, period={period:.6f}s, major_frame_len={major_frame_len}s"
        )

        while not self._stop_event.is_set():
            now = time.perf_counter()
            sleep_for = next_tick_time - now
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.perf_counter()

            elapsed = now - t0
            major_frame = int(elapsed // major_frame_len)

            within_major = elapsed - (major_frame * major_frame_len)
            tick = int((within_major / major_frame_len) * hz)
            if tick >= hz:
                tick = hz - 1

            values: Dict[str, FDAUValue] = {}
            for p in self.flt.layout:
                if self._stress_mode or tick in self._param_ticks.get(p.name, set()):
                    values[p.name] = {
                        "parameter_id": p.parameter_id,
                        "value": self._gen_value(p),
                    }

            with self._seq_lock:
                seq = self._seq
                self._seq += 1

            frame: FDAUFrame = {
                "seq": seq,
                "ts": time.time(),
                "ts_monotonic": now,
                "major_frame": major_frame,
                "tick": tick,
                "values": values,
            }

            if self.on_frame is not None:
                self.on_frame(frame)
            else:
                logger.debug(
                    f"Frame generated (no callback): major_frame={major_frame}, tick={tick}, values_count={len(values)}"
                )

            next_tick_time += period

        logger.info("FDAUUnit thread stopped")


if __name__ == "__main__":
    flt = FLTParser("flt.json").data

    def printer(frame: FDAUFrame):
        """Print non-empty output frames."""
        if frame["values"]:
            print(frame)

    fdau = FDAUUnit(flt, on_frame=printer, seed=42)
    fdau.start()

    try:
        time.sleep(3)
    finally:
        fdau.stop()
        fdau.join(timeout=2)
