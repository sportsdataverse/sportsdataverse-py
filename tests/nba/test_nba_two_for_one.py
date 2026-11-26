"""BB-D44: the 2-for-1 flag and summary on the NBA/WNBA possessions table.

Real games only: the canonical engine fixtures ``0022100001``, ``0022200001``, ``0022300001``
(``tests/fixtures/nba_engine``) and WNBA ``1022400001`` (``tests/fixtures/wnba_engine``), built into
possessions by the engine, plus the BB-D7 release captures (``tests/fixtures/bb_possession_flags``):
the whole 2023-24 NBA regular season and the whole 2025 WNBA season.

Pinned rows, counts and gates are the observed values on those fixtures with the calibrated
window and floor (``tools/models/nba_two_for_one_window.py``). Never lower a gate or re-pin a
count to make a test pass: a rule change that moves them is argued in its PR.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possession_flags import (
    POSSESSION_FLAGS_SCHEMA,
    TWO_FOR_ONE_GO_FLOOR,
    TWO_FOR_ONE_SCHEMA,
    TWO_FOR_ONE_SUMMARY_SCHEMA,
    TWO_FOR_ONE_WINDOW,
    add_possession_flags,
    add_two_for_one_flags,
    two_for_one_summary,
)
from sportsdataverse.nba.nba_possessions import build_possessions

FIX = pathlib.Path(__file__).resolve().parents[1] / "fixtures"
GAMES = {
    "0022100001": "nba_engine",
    "0022200001": "nba_engine",
    "0022300001": "nba_engine",
    "1022400001": "wnba_engine",
}
COLS = ["period", "possession_number", "change_clock", "first_shot_clock", "two_for_one", "extra_possession"]
SEQ = ["sequence_points", "sequence_points_allowed", "margin_at_change"]

#: Observed 2026-11-26 with window (33, 46] and floor 27: every window possession of each game,
#: (period, possession_number, change_clock, first_shot_clock, choice, extra, seq pts, allowed, margin).
PINNED: dict[str, list[tuple[Any, ...]]] = {
    "0022100001": [
        (3, 152, 35.0, 27.6, "went", True, 0, 2, -10),
        (4, 200, 45.1, 35.3, "went", True, 0, 0, -23),
        (4, 201, 35.3, None, "held", False, 0, 0, 23),
    ],
    "0022200001": [(2, 92, 33.1, 27.3, "went", True, 2, 2, 0), (3, 141, 45.1, 32.3, "went", True, 3, 3, 10)],
    "0022300001": [
        (1, 49, 33.4, 30.1, "went", True, 3, 0, 7),
        (2, 103, 37.2, None, "other", True, 4, 3, -18),
        (3, 153, 33.1, None, "held", False, 0, 0, -1),
        (4, 199, 34.4, 17.7, "held", True, 6, 2, 1),
    ],
    "1022400001": [(1, 38, 36.7, None, "held", True, 0, 0, -3), (4, 161, 43.3, 39.1, "went", True, 0, 4, -1)],
}

#: Observed on the season captures: went, held, other.
SEASON_COUNTS = {"nba_2024": (2476, 1274, 1073), "wnba_2025": (470, 428, 307)}

#: Sanity gates from the observed summaries (``by=("season",)``, garbage time out, late games within 10):
#: P(extra | went) 0.879 NBA / 0.847 WNBA, P(extra | held) 0.432 / 0.403. Floors and ceilings sit at
#: those values rounded against the test.
EXTRA_WENT_MIN = {"nba_2024": 0.87, "wnba_2025": 0.84}
EXTRA_HELD_MAX = {"nba_2024": 0.44, "wnba_2025": 0.41}


def _game(gid: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    payload = json.loads((FIX / GAMES[gid] / gid / "playbyplayv3.json").read_text())
    enh = enhanced_pbp_from_payload(payload, league_id="10" if gid.startswith("1") else "00")
    return build_possessions(enh), enh


def _season(key: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    lg, yr = key.split("_", 1)
    d = FIX / "bb_possession_flags"
    return pl.read_parquet(d / f"{lg}_possessions_{yr}.parquet"), pl.read_parquet(d / f"{lg}_pbp_{yr}.parquet")


@pytest.fixture(scope="module")
def seasons() -> dict[str, pl.DataFrame]:
    out = {}
    for key in SEASON_COUNTS:
        poss, pbp = _season(key)
        out[key] = add_two_for_one_flags(add_possession_flags(poss, pbp), pbp)
    return out


@pytest.mark.parametrize("gid", list(GAMES))
def test_appends_only(gid: str) -> None:
    """Same rows, order and values; the eight typed columns appended."""
    poss, pbp = _game(gid)
    out = add_two_for_one_flags(poss, pbp)
    assert out.columns == [*poss.columns, *TWO_FOR_ONE_SCHEMA]
    assert out.select(poss.columns).equals(poss)
    assert {c: out.schema[c] for c in TWO_FOR_ONE_SCHEMA} == TWO_FOR_ONE_SCHEMA
    assert out["change_clock"].null_count() == 0 and out["margin_at_change"].null_count() == 0
    # in the window every outcome is set; outside it none is
    inw = out["two_for_one"].is_not_null()
    for c in ("extra_possession", "sequence_points", "sequence_points_allowed", "sequence_net"):
        assert out.filter(inw)[c].null_count() == 0
        assert out.filter(~inw)[c].null_count() == out.filter(~inw).height


@pytest.mark.parametrize("gid", list(GAMES))
def test_pinned_window_possessions(gid: str) -> None:
    """Every window possession of the real game, pinned: a moved window or floor changes the list."""
    poss, pbp = _game(gid)
    got = (
        add_two_for_one_flags(poss, pbp)
        .filter(pl.col("two_for_one").is_not_null())
        .select(*COLS, *SEQ)
        .with_columns(pl.col("change_clock", "first_shot_clock").round(1))
        .rows()
    )
    assert got == PINNED[gid]


def test_season_counts_pin_the_constants(seasons: dict[str, pl.DataFrame]) -> None:
    """Whole-season counts: GO_FLOOR or either window end moved by a second moves them."""
    assert TWO_FOR_ONE_WINDOW == (33.0, 46.0) and TWO_FOR_ONE_GO_FLOOR == 27.0
    for key, out in seasons.items():
        vc = dict(out.group_by("two_for_one").len().drop_nulls("two_for_one").iter_rows())
        assert (vc["went"], vc["held"], vc["other"]) == SEASON_COUNTS[key]


def test_choice_definition_row_by_row(seasons: dict[str, pl.DataFrame]) -> None:
    """The sdv rule, restated: window on the change clock, went at the floor, held at a 24-second end."""
    lo, hi = TWO_FOR_ONE_WINDOW
    for out in seasons.values():
        c, s, end = pl.col("change_clock"), pl.col("first_shot_clock"), pl.col("end_seconds_remaining")
        inw = (c > lo) & (c <= hi)
        want = (
            pl.when(inw & (s >= TWO_FOR_ONE_GO_FLOOR).fill_null(False))
            .then(pl.lit("went"))
            .when(inw & (end <= 24))
            .then(pl.lit("held"))
            .when(inw)
            .then(pl.lit("other"))
        )
        assert out.select((want.eq_missing(pl.col("two_for_one"))).all()).item()
        assert out.filter(pl.col("two_for_one") == "went")["first_shot_clock"].min() >= TWO_FOR_ONE_GO_FLOOR


def _brute_sequences(out: pl.DataFrame) -> list[tuple[int, int, bool]]:
    """Rest-of-period points and the extra possession, walked possession by possession."""
    res = []
    rows = out.sort("game_id", "possession_number").to_dicts()
    for i, r in enumerate(rows):
        if r["two_for_one"] is None:
            continue
        rest = [x for x in rows[i:] if x["game_id"] == r["game_id"] and x["period"] == r["period"]]
        mine = sum(x["points"] for x in rest if x["offense_team_id"] == r["offense_team_id"])
        theirs = sum(x["points"] for x in rest[1:] if x["offense_team_id"] == r["defense_team_id"])
        extra = any(x["offense_team_id"] == r["offense_team_id"] and x["change_clock"] > 0 for x in rest[1:])
        res.append((mine, theirs, extra))
    return res


@pytest.mark.parametrize("gid", list(GAMES))
def test_sequences_match_a_possession_walk(gid: str) -> None:
    poss, pbp = _game(gid)
    out = add_two_for_one_flags(poss, pbp)
    got = (
        out.sort("game_id", "possession_number")
        .filter(pl.col("two_for_one").is_not_null())
        .select("sequence_points", "sequence_points_allowed", "extra_possession")
        .rows()
    )
    assert got == _brute_sequences(out)
    assert out.select(
        (pl.col("sequence_net") == pl.col("sequence_points") - pl.col("sequence_points_allowed")).all()
    ).item()


#: Possessions whose running possession points differ from the scoreboard: 0022200001's defensive
#: technical (Harden's free throw at order 440, inside Boston's possession 177) is in Philadelphia's
#: possession 178 points, so the points lag the scoreboard by 1 for that one possession.
POINTS_LAG = {"0022100001": [], "0022200001": [178], "0022300001": [], "1022400001": []}


@pytest.mark.parametrize("gid", list(GAMES))
def test_margin_matches_the_possession_score(gid: str) -> None:
    """The engine's possessions reconcile to the box score, so their running points are the margin."""
    poss, pbp = _game(gid)
    out = add_two_for_one_flags(poss, pbp).sort("possession_number")
    own = pl.col("points").cum_sum().over("offense_team_id") - pl.col("points")
    every = pl.col("points").cum_sum() - pl.col("points")
    off = out.filter((2 * own - every) != pl.col("margin_at_change"))
    assert off["possession_number"].to_list() == POINTS_LAG[gid]


def test_change_clock_is_add_possession_flags_clock(seasons: dict[str, pl.DataFrame]) -> None:
    """One change-of-possession clock for both flags: first_shot_clock = change_clock - seconds_to_first_shot."""
    for out in seasons.values():
        assert out.select(
            (pl.col("change_clock") - pl.col("seconds_to_first_shot")).eq_missing(pl.col("first_shot_clock")).all()
        ).item()


def test_mekonen_rules_row_by_row() -> None:
    """ACC-2026-0075's rules: rounded change clock 34-40, 7-second first shot, 24-second end, late margin 10."""
    poss, pbp = _season("nba_2024")
    out = add_two_for_one_flags(poss, pbp, rules="mekonen")
    w = out.filter(pl.col("two_for_one").is_not_null())
    assert w["change_clock"].round(0, mode="half_to_even").is_between(34, 40).all()
    assert (w.filter(pl.col("period") == 4)["margin_at_change"].abs() <= 10).all()
    sts = pl.col("change_clock") - pl.col("first_shot_clock")
    assert w.filter(pl.col("two_for_one") == "went").select((sts <= 7).all()).item()
    held = w.filter(pl.col("two_for_one") == "held")
    assert held.select((sts > 7).all()).item() and (held["end_seconds_remaining"].round(0) <= 24).all()
    assert w.height > 0 and set(w["two_for_one"]) == {"went", "held", "other"}


def test_row_order_does_not_change_the_flags() -> None:
    poss, pbp = _season("wnba_2025")
    key = ["game_id", "possession_number"]
    base = add_two_for_one_flags(poss, pbp).select(*key, *TWO_FOR_ONE_SCHEMA).sort(key)
    shuffled = add_two_for_one_flags(poss.sample(fraction=1.0, shuffle=True, seed=7), pbp.reverse())
    assert shuffled.select(*key, *TWO_FOR_ONE_SCHEMA).sort(key).equals(base)


def test_empty_and_bad_inputs() -> None:
    poss, pbp = _game("1022400001")
    empty = add_two_for_one_flags(poss.head(0), pbp.head(0))
    assert empty.height == 0 and empty.columns == [*poss.columns, *TWO_FOR_ONE_SCHEMA]
    assert {c: empty.schema[c] for c in TWO_FOR_ONE_SCHEMA} == TWO_FOR_ONE_SCHEMA
    with pytest.raises(ValueError, match="missing columns"):
        add_two_for_one_flags(poss.drop("points"), pbp)
    with pytest.raises(ValueError, match="already has"):
        add_two_for_one_flags(add_two_for_one_flags(poss, pbp), pbp)
    with pytest.raises(ValueError, match="game_id dtype"):
        add_two_for_one_flags(poss, pbp.with_columns(pl.col("game_id").cast(pl.Categorical)))
    # the opponent join matches defense_team_id against later offense_team_ids
    with pytest.raises(ValueError, match="team id dtype"):
        add_two_for_one_flags(poss.with_columns(pl.col("defense_team_id").cast(pl.Utf8)), pbp)
    with pytest.raises(ValueError, match="rules"):
        add_two_for_one_flags(poss, pbp, rules="ctg")
    with pytest.raises(ValueError, match="24-second"):
        add_two_for_one_flags(poss, pbp, shot_clock=30.0)


def test_sanity_gates(seasons: dict[str, pl.DataFrame]) -> None:
    """Going leaves time for the extra possession far more often than holding does."""
    for key, out in seasons.items():
        s = two_for_one_summary(out, by=("season",), n_boot=200)
        assert s.height == 1
        assert s["extra_rate_went"].item() >= EXTRA_WENT_MIN[key]
        assert s["extra_rate_held"].item() <= EXTRA_HELD_MAX[key]


def test_summary_binned_difference_and_interval(seasons: dict[str, pl.DataFrame]) -> None:
    """net_diff is the count-weighted mean of per-bin went-minus-held nets; the interval brackets it."""
    out = seasons["nba_2024"]
    s = two_for_one_summary(out, by=(), n_boot=300, seed=1)
    assert s.columns == list(TWO_FOR_ONE_SUMMARY_SCHEMA)
    assert {c: s.schema[c] for c in s.columns} == TWO_FOR_ONE_SUMMARY_SCHEMA
    w = out.filter(
        pl.col("two_for_one").is_in(["went", "held"])
        & (pl.col("is_garbage_time") == False)  # noqa: E712
        & ((pl.col("period") < 4) | (pl.col("margin_at_change").abs() <= 10))
    )
    per = (
        w.group_by(pl.col("change_clock").floor().alias("b"))
        .agg(
            pl.col("sequence_net").filter(pl.col("two_for_one") == "went").mean().alias("g"),
            pl.col("sequence_net").filter(pl.col("two_for_one") == "held").mean().alias("h"),
            pl.len().alias("n"),
        )
        .drop_nulls()
    )
    want = ((per["g"] - per["h"]) * per["n"]).sum() / per["n"].sum()
    assert s["net_diff"].item() == pytest.approx(want)
    assert s["n_went"].item() + s["n_held"].item() == w.height
    assert s["net_diff_lo"].item() < s["net_diff"].item() < s["net_diff_hi"].item()
    again = two_for_one_summary(out, by=(), n_boot=300, seed=1)
    assert again.equals(s)


def test_summary_per_team_and_missing_columns(seasons: dict[str, pl.DataFrame]) -> None:
    out = seasons["wnba_2025"]
    teams = two_for_one_summary(out, n_boot=50)
    assert teams.height == out["offense_team_id"].n_unique()
    assert teams.columns == ["offense_team_id", *TWO_FOR_ONE_SUMMARY_SCHEMA]
    with pytest.raises(ValueError, match="missing columns"):
        two_for_one_summary(out.drop(*POSSESSION_FLAGS_SCHEMA))
