"""Tests for FLT JSON parsing and validation."""

import json

import pytest

from core.flt import FLTParser, FLTData, FLTLayout


def write_flt(tmp_path, payload):
    """Write ``payload`` as JSON to ``tmp_path/flt.json`` and return the path."""
    path = tmp_path / "flt.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_flt_parser_loads_base_flt():
    """Bundled base FLT parses into ``FLTData`` with expected shape."""
    parser = FLTParser("data/base_flt.json")

    assert isinstance(parser.data, FLTData)
    assert parser.data.major_frame_sec == 1
    assert parser.data.minor_frames == 4
    assert len(parser.data.layout) == 20
    assert all(isinstance(item, FLTLayout) for item in parser.data.layout)


def test_flt_parser_rejects_invalid_json(tmp_path):
    """Malformed JSON raises ``JSONDecodeError``."""
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json}", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        FLTParser(str(bad))


def test_base_flt_contains_expected_parameter_names():
    """Base FLT lists known parameters in expected order and includes key names."""
    parser = FLTParser("data/base_flt.json")
    names = [item.name for item in parser.data.layout]

    assert names[:4] == ["IAS", "TAS", "Mach", "BaroAltitude"]
    assert "LandingGear" in names
    assert "AutopilotMode" in names


def test_flt_parser_propagates_file_not_found():
    """Missing file path raises ``FileNotFoundError``."""
    with pytest.raises(FileNotFoundError):
        FLTParser("data/does-not-exist.json")


def test_flt_parser_rejects_missing_required_top_level_field(tmp_path):
    """Omitted required top-level key (e.g. ``layout``) raises."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "missing layout",
        },
    )

    with pytest.raises(TypeError, match="layout"):
        FLTParser(str(flt_path))


def test_flt_parser_rejects_top_level_extra_field(tmp_path):
    """Unknown top-level keys raise (strict dataclass load)."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "extra top-level field",
            "layout": [],
            "unexpected": True,
        },
    )

    with pytest.raises(TypeError, match="unexpected"):
        FLTParser(str(flt_path))


def test_flt_parser_rejects_layout_item_with_missing_field(tmp_path):
    """Layout row missing a required field raises."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "missing parameter_id",
            "layout": [
                {
                    "name": "IAS",
                    "description": "",
                    "unit": "kt",
                    "word": 1,
                    "minor_frames": [1],
                    "hz": 1,
                    "type": "float",
                }
            ],
        },
    )

    with pytest.raises(TypeError, match="parameter_id"):
        FLTParser(str(flt_path))


def test_flt_parser_rejects_layout_item_with_extra_field(tmp_path):
    """Layout row with unknown keys raises."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "extra field in layout item",
            "layout": [
                {
                    "name": "IAS",
                    "description": "",
                    "parameter_id": 1,
                    "unit": "kt",
                    "word": 1,
                    "minor_frames": [1],
                    "hz": 1,
                    "type": "float",
                    "unexpected": "boom",
                }
            ],
        },
    )

    with pytest.raises(TypeError, match="unexpected"):
        FLTParser(str(flt_path))


def test_flt_parser_preserves_empty_layout(tmp_path):
    """Empty ``layout`` list loads as empty tuple/list."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "empty layout",
            "layout": [],
        },
    )

    parser = FLTParser(str(flt_path))

    assert parser.data.layout == []


def test_flt_parser_rejects_non_list_layout(tmp_path):
    """Non-list ``layout`` value raises with a clear message."""
    flt_path = write_flt(
        tmp_path,
        {
            "major_frame_sec": 1,
            "minor_frames": 4,
            "description": "bad layout type",
            "layout": {"name": "IAS"},
        },
    )

    with pytest.raises(TypeError, match="layout must be a list"):
        FLTParser(str(flt_path))
