"""The packaged tolerances read without PyYAML (``sportsdataverse.validation.thresholds``).

PyYAML is a dev-only dependency, so a wheel consumer cannot parse
``thresholds.yaml`` -- the reader here is the supported path and PyYAML (available
in dev) is the oracle it is pinned against.
"""

from __future__ import annotations

import pytest
import yaml

from sportsdataverse.validation import thresholds
from sportsdataverse.validation.thresholds import DEFAULTS, for_league, parse


def test_the_reader_agrees_with_pyyaml_on_the_packaged_file():
    """The consistency gate: hand parser == PyYAML on the real file, section by section."""
    reference = {k: dict(v or {}) for k, v in yaml.safe_load(thresholds.PATH.read_text()).items()}
    assert thresholds.THRESHOLDS == reference
    for section in reference:
        assert for_league(section) == {**reference["default"], **reference[section]}


def test_the_named_constants_are_the_file_s_values():
    assert thresholds.NULL_RATE_WARN == DEFAULTS["null_rate_warn"]
    assert thresholds.MEAN_SHIFT_WARN == DEFAULTS["mean_shift_warn"]
    assert thresholds.EXTRACTION_COVERAGE_FLOOR == DEFAULTS["extraction_coverage_floor"]
    assert all(isinstance(v, float) for v in DEFAULTS.values())


def test_a_league_section_overrides_the_default():
    parsed = parse("default:\n  null_rate_warn: 0.50\nnfl:\n  null_rate_warn: 0.25\ncfb: {}\n")
    assert parsed == {"default": {"null_rate_warn": 0.5}, "nfl": {"null_rate_warn": 0.25}, "cfb": {}}


def test_an_unknown_league_gets_the_defaults():
    assert for_league("xfl") == DEFAULTS
    assert for_league("NFL") == for_league("nfl")


def test_the_returned_dicts_are_copies():
    """A caller mutating its result must not move the module-level tolerances."""
    for_league("nfl")["null_rate_warn"] = 99.0
    DEFAULTS.copy()["null_rate_warn"] = 99.0
    assert for_league("nfl")["null_rate_warn"] == thresholds.NULL_RATE_WARN


@pytest.mark.parametrize(
    "text",
    [
        "default:\n  null_rate_warn: maybe\n",  # not a number
        "  null_rate_warn: 0.5\n",  # indented before any section
        "default: 0.5\n",  # a top-level scalar, not a mapping
        "default\n",  # no colon at all
    ],
)
def test_a_construct_the_reader_was_not_written_for_raises(text):
    """It must never guess: an unreadable edit is a loud failure, not a wrong tolerance."""
    with pytest.raises(ValueError):
        parse(text)


def test_the_harness_reads_the_same_values():
    from tools.validation.registry import load_thresholds

    assert load_thresholds("nfl") == for_league("nfl")
    assert load_thresholds("cfb") == for_league("cfb")
