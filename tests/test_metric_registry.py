"""The metric registry (``sportsdataverse.registry``): the packaged yaml, the resolver and the TS render."""

from __future__ import annotations

import hashlib
import importlib.resources
import subprocess
import sys

import pytest
import yaml

from sportsdataverse import registry
from sportsdataverse.registry import FORMATS, KEYS, POLARITIES, load_metric_registry, render_ts, resolve

# Game on Paper's SDV_BASE_METRIC_TITLES (astro/src/utils/constants.ts), vendored
# verbatim: every key needs an entry whose label is this title.
GOP_BASE_METRIC_TITLES = {
    "pass_rate": "Pass Rate",
    "xpass_rate": "Expected Pass Rate",
    "pass_oe": "Pass Rate Over Expected",
    "neutral_pass_rate": "Neutral Pass Rate",
    "neutral_xpass_rate": "Neutral Expected Pass Rate",
    "neutral_pass_oe": "Neutral Pass Rate Over Expected",
    "series_conv": "Series Conversion Rate",
    "fourth_decisions": "Fourth Down Decisions",
    "fourth_go_rate": "Fourth Down Go Rate",
    "fourth_go_expected": "Model Fourth Down Go Rate",
    "fourth_go_over_expected": "Fourth Down Go Rate Over Expected",
    "fourth_go_boost": "Average Fourth Down Boost",
    "fourth_go_when_recommended": "Went For It When Recommended",
    "luck_fumble_rec_pct": "Fumble Recovery Rate",
    "luck_opp_fg_pct": "Opponent Field Goal Percentage",
    "plays": "Total Plays",
    "playsgame": "Plays/Game",
    "passrate": "Pass %",
    "rushrate": "Rush %",
    "havoc": "Havoc %",
    "explosive": "Explosive %",
    "TEPA": "Total EPA",
    "EPAplay": "EPA/Play",
    "EPAdrive": "EPA/Drive",
    "EPAgame": "EPA/Game",
    "yards": "Total Yards",
    "yardsplay": "Yards/Play",
    "yardsgame": "Yards/Game",
    "play_stuffed": "Stuffed %",
    "drives": "Total Drives",
    "drivesgame": "Drives/Game",
    "yardsdrive": "Yards/Drive",
    "playsdrive": "Plays/Drive",
    "success": "Success %",
    "red_zone_success": "Red Zone SR%",
    "third_down_success": "3rd Down SR%",
    "third_down_distance": "Avg Distance (3rd)",
    "late_down_success": "Late Down SR%",
    "early_down_EPA": "Early Down EPA/Play",
    "start_position": "Starting Field Position",
    "nonExplosiveEpaPerPlay": "Non-Expl EPA/Play",
    "line_yards": "Line Yards/Rush",
    "opportunity_rate": "Rush Opp %",
    "total_available_yards": "Total Available Yards",
    "total_gained_yards": "Total Gained Yards",
    "available_yards_pct": "Available Yards %",
    "adj_epa": "Adj EPA/Play",
    "strength_faced": "Strength Faced",
    "EPAdropback": "EPA/Dropback",
    "EPArush": "EPA/Rush",
}


@pytest.fixture(scope="module")
def entries() -> list[dict]:
    return load_metric_registry()


def test_every_entry_has_the_ten_keys_and_valid_enums(entries):
    assert len(GOP_BASE_METRIC_TITLES) == 50
    assert entries
    for e in entries:
        assert tuple(e) == KEYS, e["key"]
        assert e["polarity"] in POLARITIES
        assert e["format"] in FORMATS
    keys = [e["key"] for e in entries]
    assert len(keys) == len(set(keys))


def test_the_reader_agrees_with_pyyaml_on_the_packaged_file(entries):
    """PyYAML is dev-only; the strict reader is the shipped path and PyYAML its oracle."""
    assert entries == yaml.safe_load(registry.PATH.read_text(encoding="utf-8"))


def test_a_construct_the_reader_was_not_written_for_raises():
    with pytest.raises(ValueError):
        registry.parse("key: EPAplay\n")  # no entry opener
    with pytest.raises(ValueError):
        registry.parse("- key: EPAplay\n      deep: x\n")  # six-space row outside a block


def test_a_duplicated_field_within_an_entry_raises():
    with pytest.raises(ValueError, match="duplicate field 'label'"):
        registry.parse("- key: EPAplay\n  label: a\n  label: b\n")
    with pytest.raises(ValueError, match="duplicate field 'total'"):
        registry.parse("- key: EPAplay\n  variants:\n    total: TEPA\n    total: TEPA\n")


def test_a_value_pyyaml_would_read_as_a_comment_raises():
    """``label: #x`` is null to PyYAML; the reader must not silently keep the string."""
    with pytest.raises(ValueError, match="not a plain scalar"):
        registry.parse("- key: EPAplay\n  label: #x\n")
    assert yaml.safe_load("- key: EPAplay\n  label: #x\n") == [{"key": "EPAplay", "label": None}]


def test_resolve_splits_side_phase_and_suffix():
    r = resolve("EPAplay_off_pass_rank")
    assert (r["key"], r["side"], r["phase"], r["suffix"], r["polarity"]) == (
        "EPAplay",
        "off",
        "pass",
        "_rank",
        "higher",
    )
    assert resolve("EPAplay_n")["suffix"] == "_n"
    assert resolve("EPAplay_pos_pct")["suffix"] == "_pos_pct"
    assert resolve("not_a_metric") is None
    assert resolve("EPAplay")["side"] is None


def test_def_flips_polarity_and_a_margin_is_always_higher():
    assert resolve("third_down_distance_def")["polarity"] == "higher"
    assert resolve("havoc_off")["polarity"] == "lower"
    assert resolve("havoc_def")["polarity"] == "higher"
    # every producer margin is good-minus-bad (havoc_margin = def - off)
    assert resolve("havoc_margin")["polarity"] == "higher"
    assert resolve("EPAplay_margin_rush_rank")["polarity"] == "higher"


def test_the_adjusted_columns_prefix_and_infix_spellings_resolve():
    assert resolve("adj_def_epa")["polarity"] == "lower"
    assert resolve("adj_off_epa_rank")["side"] == "off"
    assert resolve("net_adj_epa")["side"] == "margin"
    assert resolve("def_strength_faced_rank")["side"] == "def"
    # the prefix spelling is the adjusted family's alone: no other base takes it
    assert resolve("def_pass_rate") is None
    assert resolve("off_success") is None
    assert resolve("net_success") is None
    # a base key that itself ends in a suffix token still resolves
    assert resolve("available_yards_pct")["key"] == "available_yards_pct"
    assert resolve("available_yards_pct_off_pct")["suffix"] == "_pct"


def _entry(key: str, **over) -> dict:
    base = dict(zip(KEYS, (key, key, key, key, "num2", "higher", "efficiency", None, None, {})))
    return {**base, **over}


def test_an_exact_key_wins_over_a_peeled_suffix(monkeypatch):
    """A registered ``X_pct`` is its own entry, never ``X`` + ``_pct``."""
    monkeypatch.setattr(registry, "_index", lambda: {"X": _entry("X"), "X_pct": _entry("X_pct", polarity="lower")})
    assert (resolve("X_pct")["key"], resolve("X_pct")["suffix"]) == ("X_pct", None)
    assert (resolve("X_pct_def")["key"], resolve("X_pct_def")["polarity"]) == ("X_pct", "higher")
    assert (resolve("X_pct_pct")["key"], resolve("X_pct_pct")["suffix"]) == ("X_pct", "_pct")
    assert (resolve("X_rank")["key"], resolve("X_rank")["suffix"]) == ("X", "_rank")


def test_a_phase_rewrites_the_label_and_short():
    assert resolve("EPAplay_off_pass")["short"] == "EPA/DB"
    assert resolve("EPAplay_def_rush")["label"] == "EPA/Rush"
    assert resolve("success_off_pass")["short"] == "Pass SR%"
    assert resolve("passrate_off_pass")["label"] == "Pass %"  # already names the phase
    assert resolve("plays_off_rush")["label"] == "Total Rushes"


def test_resolve_returns_copies(entries):
    resolve("EPAplay")["variants"]["total"] = "x"
    assert resolve("EPAplay")["variants"]["total"] == "TEPA"


def test_every_gop_base_metric_title_is_the_entry_s_label(entries):
    labels = {e["key"]: e["label"] for e in entries}
    assert {k: labels.get(k) for k in GOP_BASE_METRIC_TITLES} == GOP_BASE_METRIC_TITLES


def test_render_ts_is_deterministic_and_headed(entries):
    first, second = render_ts(entries, target="gop"), render_ts(entries, target="gop")
    assert first == second
    assert first.startswith("// generated by sportsdataverse.registry\n")
    assert "export const METRICS" in first
    assert "export function resolveMetric" in first
    head, body = first.split("\n", 3)[:3], first.split("\n", 3)[3]
    assert head[2] == f"// sha256: {hashlib.sha256(body.encode('utf-8')).hexdigest()}"
    assert render_ts(entries, target="web") != first  # indent differs, shape does not
    assert "    key: string;" in first and "  key: string;" in render_ts(entries, target="web")
    assert "covers the body" in head[1]  # the consumer verifies the body, not the file
    with pytest.raises(ValueError, match="target"):
        render_ts(entries, target="astro")  # type: ignore[arg-type]


def test_the_cli_writes_the_same_module(entries):
    out = subprocess.run(
        [sys.executable, "-m", "sportsdataverse.registry", "--ts", "--target", "web"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert out == render_ts(entries, target="web")


def test_main_takes_target_with_or_without_ts(entries, capsys):
    from sportsdataverse.registry.__main__ import main

    assert main(["--target", "gop"]) == 0
    without = capsys.readouterr().out
    assert main(["--ts", "--target", "gop"]) == 0
    assert capsys.readouterr().out == without == render_ts(entries, target="gop")
    with pytest.raises(SystemExit):
        main(["--ts"])  # --target is required


def test_the_wheel_includes_the_yaml():
    assert importlib.resources.files("sportsdataverse.registry").joinpath("metrics.yaml").is_file()
