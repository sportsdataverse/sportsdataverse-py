"""WBB invariants: ``parse_ncaa_bb_shots(period_model=(4, 600, 300))`` +
``ncaa_mbb_join_pbp_shots`` on the committed women's fixtures.

There is deliberately NO R oracle here: wbigballR keeps ``get_shot_locations``
/ ``join_pbp_shots`` in an unexported rewrite (divergence spec §2.9) with
cruder ``4 / 90`` basket constants and MBB-halves ``Game_Seconds`` math, so
the Python WBB surface is an extension, not a port — bigballR's shared-court
``5.25 / 88.75`` baskets plus the quarter ``period_model`` knob.

Validation strategy (no cell-level oracle possible):

* exact per-game shot counts pinned to the committed captures;
* court-bounds + vocabulary invariants on every parsed column;
* the join against the Python quarter-model pbp — a 100% chart→pbp match
  rate is itself the quarter-seconds parity check (halves math would zero
  it), and made-shot score deltas confirm the made/missed flags against the
  pbp scoreboard.
"""

from __future__ import annotations

import re

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp
from sportsdataverse.mbb.mbb_ncaa_shots import (
    SHOTS_SCHEMA,
    ncaa_mbb_join_pbp_shots,
    parse_ncaa_bb_shots,
)
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR

WBB_PERIOD_MODEL = (4, 600, 300)

#: Chart shots per committed capture (== addShot call count in the fixture).
EXPECTED_SHOT_COUNTS = {
    "5722355": 129,
    "5732292": 146,
    "5728709": 136,
    "5733807": 177,
}

#: Overtime periods per fixture game (regulation ends at 2400 = 4x600s).
EXPECTED_OTS = {
    "5722355": 0,
    "5732292": 0,
    "5728709": 1,
    "5733807": 2,
}

_CLOCK_FMT = re.compile(r"^\d{2}:\d{2}$")


def _parse_shots(game_id: str) -> pl.DataFrame:
    html = (HTML_DIR / f"box_{game_id}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_shots(html, game_id, period_model=WBB_PERIOD_MODEL)


def _parse_pbp(game_id: str) -> pl.DataFrame:
    html = (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_game_pbp(html, game_id, period_model=WBB_PERIOD_MODEL)


@pytest.fixture(scope="module")
def shots() -> pl.DataFrame:
    return pl.concat([_parse_shots(g) for g in GAMES["wbb"]])


@pytest.fixture(scope="module")
def joined() -> pl.DataFrame:
    pbp = pl.concat([_parse_pbp(g) for g in GAMES["wbb"]])
    out = ncaa_mbb_join_pbp_shots(pbp, joined_shots := pl.concat([_parse_shots(g) for g in GAMES["wbb"]]))
    assert out.height == pbp.height  # join keeps every pbp row
    assert joined_shots.height > 0
    return out


@pytest.mark.parametrize("game_id", GAMES["wbb"])
def test_wbb_shot_parse_invariants(game_id: str, shots: pl.DataFrame) -> None:
    got = shots.filter(pl.col("game_id") == game_id)
    assert got.schema == pl.Schema(SHOTS_SCHEMA)
    assert got.height == EXPECTED_SHOT_COUNTS[game_id]

    # court bounds: feet on the 94x50 canvas; distance below the court diagonal
    assert got.filter((pl.col("x") < 0) | (pl.col("x") > 94)).height == 0
    assert got.filter((pl.col("y") < 0) | (pl.col("y") > 50)).height == 0
    assert got.filter((pl.col("shot_dist") <= 0) | (pl.col("shot_dist") > 107)).height == 0

    # quarter model: periods 1..4 + OTs; game_seconds within the period span
    max_seconds = 2400 + 300 * EXPECTED_OTS[game_id]
    assert got["period"].max() == 4 + EXPECTED_OTS[game_id]
    assert got["period"].min() == 1
    assert got.filter((pl.col("game_seconds") <= 0) | (pl.col("game_seconds") > max_seconds)).height == 0

    # vocabulary / completeness
    assert set(got["shot_result"].unique().to_list()) <= {"made", "missed"}
    assert got["team"].n_unique() == 2
    assert got["player"].null_count() == 0
    assert all(_CLOCK_FMT.fullmatch(c) is not None for c in got["clock"].to_list())


@pytest.mark.parametrize("game_id", GAMES["wbb"])
def test_wbb_join_match_rate_and_na_fill(game_id: str, joined: pl.DataFrame, shots: pl.DataFrame) -> None:
    jg = joined.filter(pl.col("game_id") == game_id)
    sg = shots.filter(pl.col("game_id") == game_id)

    # every chart shot found its pbp FG row — this is the quarter-seconds
    # parity gate (MBB-halves math on either side would zero the match rate)
    matched = jg.filter(pl.col("x").is_not_null()).height
    assert matched == sg.height

    # unmatched pbp rows are kept, NA-filled: non-FG rows never carry coords
    non_fg = jg.filter(pl.col("shot_value").is_null() | (pl.col("shot_value") == 1))
    assert non_fg["x"].null_count() == non_fg.height
    assert non_fg["shot_dist"].null_count() == non_fg.height


def test_wbb_made_flags_consistent_with_score_deltas(joined: pl.DataFrame) -> None:
    """Matched made FGs move the pbp scoreboard by their shot value."""
    total = pl.col("home_score") + pl.col("away_score")
    made = joined.with_columns((total - total.shift(1).over("game_id")).alias("__delta")).filter(
        (pl.col("x").is_not_null()) & (pl.col("event_result") == "made")
    )
    assert made.height > 0
    assert set(made["__delta"].drop_nulls().unique().to_list()) <= {0, 2, 3}
    ok = made.filter(pl.col("__delta") == pl.col("shot_value")).height
    # observed 234/235 on the committed fixtures (one scorekeeping stutter)
    assert ok / made.height >= 0.99
