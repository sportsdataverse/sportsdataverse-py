"""The summary/leaderboard ``_n`` sample-size descriptions, as SHIPPED.

Asserts against the committed artifacts -- ``manual_column_descriptions.yaml``
and ``schemas/loader_schemas.yaml`` -- not the generators, so a wrong noun that
reaches the returns tables fails here. The sample sizes themselves come from
cfbfastR-cfb-data#30/#33; the NFL contrasts are transcribed from nfl-data's
``nfl_team_summaries/build.py`` ``PLAYER_SAMPLE_SIZES``.
"""

from pathlib import Path

import pytest
import yaml

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
_LOADERS = (
    "load_cfb_team_summaries",
    "load_cfb_team_summaries_weekly",
    "load_cfb_passing",
    "load_cfb_rushing",
    "load_cfb_receiving",
)


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def declared_n() -> dict[str, list[str]]:
    schemas = yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))
    return {t: [c["name"] for c in schemas[t] if c["name"].endswith("_n")] for t in _LOADERS}


def test_every_declared_n_column_is_described(manual, declared_n):
    missing = [f"{t}.{c}" for t, cols in declared_n.items() for c in cols if not (manual.get(t) or {}).get(c)]
    assert not missing, missing
    # 140 + the four Five Factors counts (cfbfastR-cfb-data#103)
    assert len(declared_n["load_cfb_team_summaries"]) == 144


def test_weekly_n_texts_equal_team_summaries(manual, declared_n):
    weekly = declared_n["load_cfb_team_summaries_weekly"]
    assert len(weekly) == 144
    season, week = manual["load_cfb_team_summaries"], manual["load_cfb_team_summaries_weekly"]
    assert {c: week[c] for c in weekly} == {c: season[c] for c in weekly}


_TAIL = "the passer's value is computed over."
_ATT = "completions and incompletions (interceptions and sacks excluded)"
_DROPBACKS = "dropbacks (completions, incompletions, sacks and interceptions)"


@pytest.mark.parametrize(
    "loader,col,text",
    [
        (
            "load_cfb_passing",
            "success_n",
            f"Sample size behind success: the number of {_ATT} {_TAIL} 0 where success is null. "
            "nfl-data's nfl_passing table counts its success_n over dropbacks instead.",
        ),
        (
            "load_cfb_passing",
            "yardsplay_n",
            f"Sample size behind yardsplay: the number of {_ATT} {_TAIL} 0 where yardsplay is null. "
            "nfl-data's nfl_passing table counts its yardsplay_n over pass attempts instead: "
            "interceptions included, sacks still excluded.",
        ),
        (
            "load_cfb_passing",
            "comppct_n",
            "Sample size behind comppct: the number of throws (including interceptions, excluding sacks) "
            f"{_TAIL} 0 where comppct is null.",
        ),
        (
            "load_cfb_passing",
            "EPAplay_n",
            f"Sample size behind EPAplay: the number of {_DROPBACKS} {_TAIL} 0 where EPAplay is null.",
        ),
        (
            "load_cfb_rushing",
            "success_n",
            "Sample size behind success: the number of carries the rusher's value is computed over. "
            "0 where success is null.",
        ),
        (
            "load_cfb_receiving",
            "catchpct_n",
            "Sample size behind catchpct: the number of targets the receiver's value is computed over. "
            "0 where catchpct is null.",
        ),
        (
            "load_cfb_team_summaries",
            "line_yards_off_pass_n",
            "Sample size behind line_yards_off_pass: the number of rushes it is computed over on pass plays, "
            "with the team on offense. Null when the team has no rows in that split; read it as 0. "
            "Always 0 here: line yards are credited only on rushes.",
        ),
    ],
)
def test_shipped_n_text(manual, loader, col, text):
    assert manual[loader][col] == text
