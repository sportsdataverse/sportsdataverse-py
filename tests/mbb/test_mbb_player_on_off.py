"""BB-D5: college player on/off and WOWY pairs, on committed real captures.

Fixtures (none synthetic): the BB-D6 contests of 2024-25 (12 men's D1 games plus one against a
non-D1 visitor, 12 women's) from ``tests/fixtures/bb_lineup_seasons`` (``ncaa_{mbb,wbb}_pbp`` and
``ncaa_{mbb,wbb}_possessions``), and their final points from ``tests/fixtures/bb_game_impact``
(``ncaa_{mbb,wbb}_team_box``). A "season" here is a team's fixture games. The team totals come
from the possessions release and the team box, not from the impact rows the builders read. There
is no public college on/off to check against; the rating arithmetic is the NBA's, checked against
stats.nba.com in ``tests/nba/test_nba_player_on_off.py``.
"""

from __future__ import annotations

import importlib
from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.player_on_off import WOWY_SIDES, player_on_off, player_wowy
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
from sportsdataverse.mbb.mbb_player_on_off import mbb_player_on_off, mbb_player_wowy
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_player_on_off import wbb_player_on_off, wbb_player_wowy

adapter = importlib.import_module("sportsdataverse.mbb.mbb_player_on_off")

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
LEAGUES = {"mbb": "mens", "wbb": "womens"}


def _read(lg: str, name: str, corpus: str = "bb_lineup_seasons") -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / corpus / f"ncaa_{lg}_{name}_2025.parquet")


@cache
def _rows(lg: str) -> pl.DataFrame:
    return ncaa_impact_rows(_read(lg, "pbp"), _read(lg, "possessions"), league=LEAGUES[lg])


def _on_off(lg: str, *, min_poss: int, level: str | None = None) -> pl.DataFrame:
    dates = adapter._game_dates(_read(lg, "pbp"))
    return player_on_off(_rows(lg), player_dtype=pl.Utf8(), min_poss=min_poss, level=level, game_dates=dates)


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_span_off_is_the_team_over_his_span(lg: str) -> None:
    """The span columns from the pbp's game dates: on + off_span is the team over the span's games."""
    out = _on_off(lg, min_poss=20)
    assert out["team_games_span"].null_count() == 0 and out.schema["span_first_date"] == pl.Date
    dates = adapter._game_dates(_read(lg, "pbp"))
    assert dates.height == _read(lg, "pbp")["contest_id"].n_unique()  # every contest has its date
    poss = _read(lg, "possessions")
    side = pl.when(pl.col("poss_team") == pl.col("home")).then(pl.col("home_espn_team_id"))
    team_id = side.when(pl.col("poss_team") == pl.col("away")).then(pl.col("away_espn_team_id"))
    per_game = (
        poss.group_by(team_id.cast(pl.Int64).alias("team_id"), pl.col("contest_id").cast(pl.Utf8).alias("game_id"))
        .agg(pl.len().cast(pl.Int64).alias("n"))
        .join(dates, on="game_id", validate="m:1")
    )
    span = (
        out.select("team_id", "player_id", "span_first_date", "span_last_date")
        .join(per_game, on="team_id")
        .filter(pl.col("game_date") >= pl.col("span_first_date"), pl.col("game_date") <= pl.col("span_last_date"))
        .group_by("team_id", "player_id")
        .agg(pl.col("n").sum().alias("span_poss"))
    )
    j = out.join(span, on=["team_id", "player_id"], validate="1:1")
    assert j.height == out.height and (j["poss_on"] + j["poss_off_span"] == j["span_poss"]).all()
    assert (out["poss_off_span"] <= out["poss_off"]).all()


def _team_totals(lg: str) -> pl.DataFrame:
    """Each D1 team's fixture possessions (the possessions release) and points (the team box)."""
    poss = _read(lg, "possessions")
    side = pl.when(pl.col("poss_team") == pl.col("home")).then(pl.col("home_espn_team_id"))
    team_id = side.when(pl.col("poss_team") == pl.col("away")).then(pl.col("away_espn_team_id"))
    counted = poss.group_by(team_id.cast(pl.Int64).alias("team_id")).agg(pl.len().cast(pl.Int64).alias("team_poss"))
    box = _read(lg, "team_box", "bb_game_impact").group_by(pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id"))
    pts = box.agg(pl.col("pts").cast(pl.Int64).sum().alias("team_pts"))
    return counted.join(pts, on="team_id", validate="1:1").drop_nulls("team_id")


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_on_plus_off_is_the_team_season(lg: str) -> None:
    out = player_on_off(_rows(lg), player_dtype=pl.Utf8(), min_poss=20)
    assert out.schema["team_id"] == pl.Int64 and out.schema["player_id"] == pl.Utf8
    j = out.join(_team_totals(lg), on="team_id", how="left", validate="m:1")
    assert j["team_poss"].null_count() == 0
    assert (j["poss_on"] + j["poss_off"] == j["team_poss"]).all()
    assert (j["pts_on"] + j["pts_off"] == j["team_pts"]).all()
    # every D1 team of the fixture and nobody else: the men's non-D1 visitor has no id, so no rows
    assert set(out["team_id"]) == set(_team_totals(lg)["team_id"]) and out["team_id"].n_unique() == 24


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_wowy_splits_add_up_to_the_team(lg: str) -> None:
    w = player_wowy(_rows(lg), player_dtype=pl.Utf8())
    j = w.join(_team_totals(lg), on="team_id", validate="m:1")
    assert j.select((sum(pl.col(f"poss_{s}") for s in WOWY_SIDES) == pl.col("team_poss")).all()).item()
    assert j.select((sum(pl.col(f"pts_{s}") for s in WOWY_SIDES) == pl.col("team_pts")).all()).item()
    assert (w["player_id_a"] < w["player_id_b"]).all()  # string order for the NCAA ids
    n = player_on_off(_rows(lg), player_dtype=pl.Utf8(), min_poss=20).group_by("team_id").agg(pl.len().alias("n"))
    assert w.height == n.select((pl.col("n") * (pl.col("n") - 1) // 2).sum()).item()


def test_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(lg: str, name: str):  # type: ignore[no-untyped-def]
        def load(seasons: int | list[int]) -> pl.DataFrame:
            assert seasons == 2025, f"{name}: called with {seasons}"
            return _read(lg, name)

        return load

    for lg, mod in (("mbb", mbb_loaders), ("wbb", wbb_loaders)):
        monkeypatch.setattr(mod, f"load_ncaa_{lg}_pbp", fixture(lg, "pbp"))
        monkeypatch.setattr(mod, f"load_ncaa_{lg}_possessions", fixture(lg, "possessions"))
    seen: list[object] = []
    real = adapter.player_on_off

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(kwargs["level"])
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "player_on_off", spy)
    gated = _on_off("mbb", min_poss=20, level="mbb")
    assert mbb_player_on_off(2025, min_poss=20).equals(gated)
    assert not gated.equals(_on_off("mbb", min_poss=20))  # the level gate bites
    # no WBB fixture player reaches the 200-possession floor both ways, so nothing ranks gated or
    # ungated here; the mbb comparison above is the one that bites
    assert wbb_player_on_off(2025).equals(_on_off("wbb", min_poss=200, level="wbb"))
    assert seen == ["mbb", "wbb"]  # the WBB fixture ranks nobody, so only the spy can see its level
    assert mbb_player_wowy(2025).equals(player_wowy(_rows("mbb"), player_dtype=pl.Utf8()))
    assert wbb_player_wowy(2025).equals(player_wowy(_rows("wbb"), player_dtype=pl.Utf8()))
    with pytest.raises(ValueError, match="league"):
        mbb_player_on_off(2025, league="coed")
