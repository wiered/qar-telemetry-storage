"""FLT JSON parsing and typed layout/parameter metadata."""

import dataclasses
import json
from logging import getLogger

logger = getLogger(__name__)


class FLTParser:
    """Parses FLT JSON files into typed dataclasses.

    Args:
        flt_file: Path to the FLT JSON file.
    """

    def __init__(self, flt_file: str):
        logger.info(f"Initializing FLTParser with file: {flt_file}")
        self.flt_file = flt_file
        self.data = self.parse()

    def parse(self) -> "FLTData":
        """Reads the FLT file and converts it into `FLTData`.

        Returns:
            Parsed FLT data object.

        Raises:
            TypeError: If `layout` is present and is not a list.
        """
        logger.info(f"Parsing FLT file: {self.flt_file}")
        with open(self.flt_file, "r", encoding="utf-8") as file:
            data = json.load(file)
            if "layout" in data:
                if not isinstance(data["layout"], list):
                    raise TypeError("layout must be a list")
                data["layout"] = [FLTLayout(**item) for item in data["layout"]]
            logger.info("FLT file parsed successfully")
            return FLTData(**data)


@dataclasses.dataclass
class FLTLayout:
    """Describes a single parameter layout entry in FLT data."""

    name: str
    description: str
    parameter_id: int
    unit: str
    word: int
    minor_frames: list[int]
    hz: int
    type: str


@dataclasses.dataclass
class FLTData:
    """Top-level FLT payload with frame metadata and layout entries."""

    major_frame_sec: int
    minor_frames: int
    description: str
    layout: list[FLTLayout]
