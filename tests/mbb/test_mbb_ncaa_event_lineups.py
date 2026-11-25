"""Event-level lineups: each pbp row carries the five on the floor at that row (real pages only).

bigballR froze every row on its possession's first lineup and paired subs by queue position, so
a mid-possession dead-ball sub was lost until the next possession (bigballR issue #48). These
tests run the parser both ways on committed stats.ncaa.org captures:
``tests/fixtures/ncaa/bigballr/html`` (8 games, 2019-2026) and
``tests/fixtures/ncaa/event_lineups`` (2 games from 2026, see its README).
The independent oracle is the box score's minutes played (``individual_stats`` page, ``MP``).
"""

from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"
HALVES, QUARTERS = (2, 1200, 300), (4, 600, 300)
GAMES = {
    "6470186": ("bigballr/html", HALVES),
    "6479639": ("bigballr/html", HALVES),
    "6479592": ("bigballr/html", HALVES),
    "1613299": ("bigballr/html", HALVES),
    "5722355": ("bigballr/html", QUARTERS),
    "5732292": ("bigballr/html", QUARTERS),
    "5728709": ("bigballr/html", QUARTERS),
    "5733807": ("bigballr/html", QUARTERS),
    "6416178": ("event_lineups", HALVES),
}
SUBS = ("Enters Game", "Leaves Game")
_SUFFIX = {"JR", "SR", "II", "III", "IV"}


def _parse(game: str, *, event_lineups: bool = True) -> pl.DataFrame:
    folder, model = GAMES.get(game, ("event_lineups", HALVES))
    html = (FIX / folder / f"pbp_{game}.html").read_text()
    return parse_ncaa_bb_game_pbp(html, game, period_model=model, event_lineups=event_lineups)


def _five() -> pl.Expr:
    side = pl.col("event_team") == pl.col("home")
    home = pl.concat_list([f"home_{i}" for i in range(1, 6)])
    return pl.when(side).then(home).otherwise(pl.concat_list([f"away_{i}" for i in range(1, 6)]))


def _actors_off(p: pl.DataFrame) -> int:
    """Rows whose actor (not a sub, not a free throw: substitute shooters are legal) is off his five."""
    acts = p.filter(~pl.col("event_type").is_in([*SUBS, "Free Throw"]) & pl.col("player_1").is_not_null())
    acts = acts.filter(pl.col("player_1") != "TEAM")
    return acts.filter(~_five().list.contains(pl.col("player_1"))).height


def _key(name: object) -> str:
    """A name both pages agree on: upper-case letters only, suffixes dropped, tokens sorted."""
    tokens = re.sub(r"[^A-Z]+", " ", str(name).upper()).split()
    return " ".join(sorted(t for t in tokens if t not in _SUFFIX))


def _minutes_error(game: str, p: pl.DataFrame) -> float:
    """Mean |our on-court minutes - the box's MP| over the players both pages name."""
    folder = GAMES.get(game, ("event_lineups",))[0]
    box: dict[str, int] = {}
    for t in pd.read_html(io.StringIO((FIX / folder / f"individual_stats_{game}.html").read_text())):
        if "MP" in [str(c) for c in t.columns]:
            for name, mp in zip(t["Name"], t["MP"]):
                m = re.fullmatch(r"(\d+):(\d+)", str(mp).strip())
                if m:
                    box[_key(name)] = int(m[1]) * 60 + int(m[2])
    dt = (pl.col("game_seconds").shift(-1).over("period") - pl.col("game_seconds")).fill_null(0).clip(lower_bound=0)
    p = p.with_columns(dt.alias("_dt"))
    ours: dict[str, int] = {}
    for col in [f"{s}_{i}" for s in ("home", "away") for i in range(1, 6)]:
        for name, secs in p.select(col, "_dt").iter_rows():
            if name is not None:
                ours[_key(name)] = ours.get(_key(name), 0) + secs
    errs = [abs(ours[k] - v) / 60 for k, v in box.items() if v > 0 and k in ours]
    assert len(errs) >= 15  # most of the box matched by name
    return sum(errs) / len(errs)


@pytest.mark.parametrize("game", list(GAMES))
def test_every_actor_is_on_his_five_at_his_act(game: str) -> None:
    assert _actors_off(_parse(game)) == 0
    # the bigballR walk (possession freeze + queue-paired subs) left some off
    assert _actors_off(_parse(game, event_lineups=False)) > 0


@pytest.mark.parametrize("game", list(GAMES))
def test_minutes_on_the_floor_match_the_box_score_closer(game: str) -> None:
    event, bigballr = _minutes_error(game, _parse(game)), _minutes_error(game, _parse(game, event_lineups=False))
    assert event <= bigballr
    assert event < 0.6  # observed 0.04-0.57 a player (the overtime games are the widest)


def test_a_free_thrower_stays_on_until_his_trip_ends() -> None:
    """6416178: Udemadu's Leaves Game is listed before his own two free throws at 2389 s.

    A free thrower cannot be replaced (Rule 3-6.2.a), so he counts on the floor for both, and the
    sub takes effect after them. bigballR, applying rows in page order, had him off for both.
    """
    for event_lineups, on in ((True, True), (False, False)):
        p = _parse("6416178", event_lineups=event_lineups)
        trip = p.filter(
            (pl.col("game_seconds") == 2389)
            & (pl.col("event_type") == "Free Throw")
            & (pl.col("player_1") == "ALLEN.UDEMADU")
        )
        assert trip.height == 2
        assert trip.select(_five().list.contains(pl.col("player_1"))).to_series().to_list() == [on, on]


def test_a_placeholder_sub_no_longer_drops_the_games_lineups() -> None:
    """6424024 logs one sub for ``UNKNOWN.TEAM``: bigballR shipped the whole game with null lineups."""
    old, new = _parse("6424024", event_lineups=False), _parse("6424024")
    assert old["status"][0] == "NO_PLAYER" and old["home_1"].null_count() == old.height
    assert new["home_1"].null_count() == 0 and new["away_1"].null_count() == 0
    assert _actors_off(new) == 0


@pytest.mark.parametrize("game", list(GAMES))
def test_a_team_never_has_more_than_five(game: str) -> None:
    p = _parse(game)
    for side in ("home", "away"):
        n = pl.concat_list([f"{side}_{i}" for i in range(1, 6)]).list.drop_nulls().list.unique().list.len()
        assert p.select(n.max()).item() <= 5
        # a name appears once per five
        assert (
            p.select(n).to_series()
            == p.select(pl.concat_list([f"{side}_{i}" for i in range(1, 6)]).list.drop_nulls().list.len()).to_series()
        ).all()
