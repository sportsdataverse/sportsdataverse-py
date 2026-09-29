"""The tendencies split-grid descriptions (``gen_tendencies_descriptions.py``).

The split / context columns are declared in loader_schemas.yaml from the
republished assets (an undeclared description key is an orphan). This pins that
the generator describes every column the producer emits, and -- live, behind
``SDV_PY_LIVE_TESTS=1`` -- that the declared schema is the published one.
"""

import re
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.football.tendencies import CONTEXTS, tendencies
from tests.conftest import skip_if_no_live
from tools.codegen.gen_tendencies_descriptions import TARGETS, describe

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def emitted() -> list[str]:
    """Every column of a ``tendencies()`` row whose plays carry all the context inputs."""
    ctx = {f"{p}ctx_{c}": [True, False] for p in ("", "def_") for c in (*CONTEXTS, "win")}
    plays = pl.DataFrame(
        {
            "season": 2025,
            "game_id": [1, 1],
            "pos_team": [1, 2],
            "def_pos_team": [2, 1],
            "drive.id": ["1", "2"],
            "start.down": [3, 1],
            "start.distance": [5, 10],
            "start.yardsToEndzone": [30, 70],
            "scrimmage_play": True,
            "penalty_no_play": False,
            **ctx,
        }
    )
    return tendencies(plays, league="cfb", third_down_curve=pl.DataFrame({"distance": [], "rate": []})).columns


@pytest.mark.parametrize("loader", sorted(TARGETS))
def test_every_emitted_column_is_described(manual, emitted, loader):
    have = manual[loader]
    blank = [c for c in emitted if c not in ("season", "pos_team") and not have.get(c) and not describe(c)]
    assert not blank, blank


def test_templates_reproduce_the_shipped_split_text(manual):
    """Where the grid was hand-described, the template reads the same."""
    team = manual["load_nfl_team_tendencies"]
    for col in ("plays_d1", "passes_d1", "pass_rate_d1", "def_plays_d1", "def_pass_rate_d1", "plays_tied"):
        assert describe(col) == team[col], col
    coach = manual["load_cfb_coach_tendencies"]
    assert describe("def_plays_d1", "coach") == coach["def_plays_d1"]


@pytest.mark.parametrize(
    "col,text",
    [
        (
            "epa_per_play_d3_long",
            "epa_d3_long / plays_d3_long: EPA per play on third down with 7 or more yards to go. "
            "Null when the denominator is 0.",
        ),
        (
            "def_plays_opp_half",
            "Defense-allowed twin of plays_opp_half -- the same measure over the opposing offenses' plays while "
            "this team's defense was on the field: plays snapped in the opponent's half (fewer than 50 yards "
            "from the opponent end zone).",
        ),
        (
            "def_wins_home",
            "Defense-allowed twin of wins_home -- the same measure over the opposing offenses' plays while this "
            "team's defense was on the field, with the game context read from the defending team's side "
            "(def_ctx_*): of games_home, the home games the team won (points for above points against, so a "
            "tie is not a win).",
        ),
        (
            "win_rate_after_bye",
            "wins_after_bye / games_after_bye: win rate in the team's games played 13 or more "
            "days after the team's previous game. Null when the denominator is 0.",
        ),
    ],
)
def test_exact_text(col, text):
    assert describe(col) == text


_AT_SNAP = "the score at the snap, before the play; pos_score_diff where the start score is missing"


@pytest.mark.parametrize(
    "col,text",
    [
        (
            "epa_per_play_leading",
            "epa_leading / plays_leading: EPA per play snapped with the offense ahead on the scoreboard "
            f"(pos_score_diff_start > 0, {_AT_SNAP}). Null when the denominator is 0.",
        ),
        ("plays_tied", f"Plays snapped with the score tied (pos_score_diff_start == 0, {_AT_SNAP})."),
        (
            "plays_one_score",
            f"Plays snapped with the offense within 8 points either way (|pos_score_diff_start| <= 8, {_AT_SNAP}).",
        ),
        (
            "plays_red_zone",
            "Plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather "
            "than the absolute yard line; the play-by-play rz_play flag only when that distance is missing).",
        ),
    ],
)
def test_score_state_and_red_zone_text(col, text):
    """The republish moved these on purpose: score at the snap, red zone from yards to goal."""
    assert describe(col) == text


@pytest.mark.parametrize("loader", sorted(TARGETS))
def test_shipped_score_state_text_is_the_template(manual, loader):
    """The merge is additive, so the older leading/tied/trailing text had to be replaced, not kept."""
    moved = re.compile(r"_(leading|tied|trailing|one_score|red_zone)$")
    stale = [c for c, d in manual[loader].items() if moved.search(c) and describe(c, TARGETS[loader]) not in (None, d)]
    assert not stale, stale


def test_off_grid_columns_keep_their_hand_text():
    for col in ("plays", "epa", "def_epa_rush", "rz_trips", "games", "go_rate", "plays_per_game", "epa_per_pass"):
        assert describe(col) is None, col


@skip_if_no_live
@pytest.mark.parametrize("fn", sorted(TARGETS))
def test_declared_schema_matches_the_published_parquet(fn):
    """The declared returns-schema is the REAL published asset (2025; careers is one season-less asset)."""
    import sportsdataverse.cfb as cfb
    import sportsdataverse.nfl as nfl

    load = getattr(cfb if fn.startswith("load_cfb") else nfl, fn)
    df = load() if fn.endswith("_careers") else load(seasons=2025)
    declared = {
        c["name"]: c["type"] for c in yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text())[fn]
    }

    assert df.height > 0, fn
    assert set(df.columns) == set(declared), (
        f"missing={sorted(set(declared) - set(df.columns))} extra={sorted(set(df.columns) - set(declared))}"
    )
    drift = {c: (declared[c], str(df.schema[c])) for c in df.columns if str(df.schema[c]) != declared[c]}
    assert not drift, drift
