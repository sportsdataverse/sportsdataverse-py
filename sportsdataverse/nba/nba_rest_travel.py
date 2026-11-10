"""NBA / WNBA rest, back-to-backs and travel before each game (BB-D23), and their effect on the margin.

Everything here is read off the schedule, so it is known before the tip: for each team and game,
the days since its previous game, whether it is the second night of a back-to-back or the third
game in four nights, and how far it travelled and how many time zones it crossed since its
previous game. :func:`rest_travel_margin` turns the home-minus-away differences into points of
expected home margin, which the season sim (:mod:`sportsdataverse.nba.nba_season_sim`) adds to
each scheduled game.

**Where a game is played.** ``sportsdataverse/nba/data/game_sites.csv`` (one row per team's home
arena per season range, plus the neutral sites and the 2020 bubbles, each with a source). A
neutral-site game (:mod:`sportsdataverse.nba.nba_neutral_site`) is at its listed city, a game
inside a 2020 bubble at the bubble, and every other game at the home team's arena that season.
A team's designated home games at another of its own sites (the Spurs in Austin, the Tempo in
Vancouver) are placed at its home arena. Coordinates are rounded to 0.01 degree (about 1 km).

**Features**, per team and game, over the team's regular-season and Cup games in date order:

* ``rest_days``: days off since the previous game (0 on a back-to-back), capped at
  :data:`REST_CAP`; a season opener is capped.
* ``b2b``: ``rest_days == 0``.
* ``three_in_four``: the third game in four nights (two games back was at most three days ago).
* ``travel_km``: great-circle distance from the previous game's site (a season opener: from home).
* ``tz_shift``: hours between the UTC offsets of the previous and this game's sites, both on this
  game's date (so Phoenix in the summer is Pacific time, and a daylight-saving switch is no shift).

**The effect** (:data:`REST_TRAVEL_CONSTANTS`): ``sum(coef * (home - away))``, points
of home margin, ``travel`` in thousands of km. Fit by ``tools/fit_rest_travel.py`` on the
residual of the season sim's own game model, walk-forward by season; see that constant for the
held-out numbers. A league with no entry gets no adjustment.
"""

from __future__ import annotations

import datetime as dt
import io
from functools import lru_cache
from importlib.resources import files
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_neutral_site import BUBBLES, nba_neutral_site_games

__all__: list[str] = []

#: ``rest_days`` cap: two or more days off is "rested".
REST_CAP = 2

#: The home-minus-away differences the effect is fit on, in :data:`REST_TRAVEL_CONSTANTS` order.
FEATURES = ("b2b", "three_in_four", "rest_days", "travel", "tz_shift")

#: league_id -> one coefficient per :data:`FEATURES`, in points of home margin per unit of
#: home-minus-away difference (``travel`` per 1000 km). Fit by ``tools/fit_rest_travel.py``
#: (2026-11-10; result JSON in ClaudeCowork ``data/2026-11-10-bb-d23-rest-travel/``): OLS with no
#: intercept of the residual of the season sim's game model (the as-of ridge with the preseason
#: prior, weekly cutoffs), one row per game at its latest cutoff. Walk-forward: each holdout season
#: is scored with the fit on the seasons before it only, on every (cutoff, game) pair. The gate,
#: set before any fit and never lowered: log loss, Brier and margin MAE all better than the game
#: model alone on at least two holdout seasons. Frozen; refit when a season is added to the train
#: set, and check 2026-27 out of sample at its end (no clean NBA holdout is left).
#:
#: * NBA, train 2018 2019 2022 2023, holdout 2024 2025 2026: passes, 3 of 3. Paired differences
#:   (with rest and travel minus without, SE clustered by game), log loss / Brier / margin MAE:
#:   2024 -0.00349 +- 0.00218 / -0.00189 +- 0.00096 / -0.072 +- 0.035 (b2b fit -1.84);
#:   2025 -0.00351 +- 0.00226 / -0.00136 +- 0.00101 / -0.048 +- 0.037 (b2b -1.79);
#:   2026 -0.00185 +- 0.00230 / -0.00064 +- 0.00103 / -0.053 +- 0.039 (b2b -2.07; on its own not
#:   distinguishable from zero); pooled -0.00295 +- 0.00130 / -0.00130 +- 0.00058 / -0.058 +- 0.021.
#:   Calibration slopes stay within 0.03 of the base's (0.98-1.03); by back-to-back difference the
#:   base overrates the tired team by about 6 points of probability and the adjusted model by 1-2.
#:   The shipped values are the fit on all seven seasons (8,610 games; SE b2b 0.48, three_in_four
#:   0.30, rest_days 0.30, travel 0.20, tz_shift 0.26), never scored out of sample themselves. The
#:   back-to-back (4 SE) and three-in-four (2 SE) carry it; rest_days is noise, and travel and
#:   time zones lean on each other (1.4 and 1.8 SE, opposite signs). A first run with an intercept
#:   (-0.32 +- 0.17, a home-term change) passed the same way; it was dropped after seeing its
#:   holdout, since alone it did nothing (``--intercept`` reproduces it).
#: * WNBA, train 2018 2019 2021, holdout 2024 2025 (2022, 2023 and 2026 stay reserved by the
#:   2026-11-02 sim-vs-market spec): fails, 1 of 2 (2025 paired log loss +0.0113 +- 0.0062, MAE
#:   +0.26 +- 0.09). Few WNBA back-to-backs, 1,126 games; no entry, so no adjustment.
REST_TRAVEL_CONSTANTS: dict[str, dict[str, float]] = {
    "00": {"b2b": -1.9391, "three_in_four": -0.6171, "rest_days": 0.1004, "travel": 0.2893, "tz_shift": -0.4702},
}

_SITE_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8(),
    "kind": pl.Utf8(),
    "team_id": pl.Int64(),
    "team": pl.Utf8(),
    "first_season": pl.Int64(),
    "last_season": pl.Int64(),
    "place": pl.Utf8(),
    "arena": pl.Utf8(),
    "lat": pl.Float64(),
    "lon": pl.Float64(),
    "tz": pl.Utf8(),
    "source_url": pl.Utf8(),
}
_LEAGUE = {"00": "nba", "10": "wnba"}
_EARTH_KM = 6371.0

#: :func:`team_game_rest_travel` output, one row per team and game.
TEAM_GAME_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "rest_days": pl.Int64(),
    "b2b": pl.Int64(),
    "three_in_four": pl.Int64(),
    "travel_km": pl.Float64(),
    "tz_shift": pl.Float64(),
}


@lru_cache(maxsize=1)
def game_sites() -> pl.DataFrame:
    """The bundled ``game_sites.csv``: home arenas by season range, neutral sites and the bubbles."""
    raw = (files("sportsdataverse.nba") / "data" / "game_sites.csv").read_bytes()
    return pl.read_csv(io.BytesIO(raw), schema=_SITE_SCHEMA)


@lru_cache(maxsize=4096)
def _utc_offset_hours(tz: str, day: dt.date) -> float:
    off = dt.datetime(day.year, day.month, day.day, 12, tzinfo=ZoneInfo(tz)).utcoffset()
    return 0.0 if off is None else off.total_seconds() / 3600.0


def _home_sites(frame: pl.DataFrame, team_col: str, league_id: str) -> pl.DataFrame:
    """``frame`` with ``h_lat`` / ``h_lon`` / ``h_tz``: the ``team_col`` team's home arena that ``season``.

    Raises:
        ValueError: If a team has no ``game_sites.csv`` home row for the season.
    """
    homes = game_sites().filter((pl.col("league") == _LEAGUE[league_id]) & (pl.col("kind") == "home"))
    indexed = frame.with_row_index("_r")
    hit = (
        indexed.select("_r", team_col, "season")
        .join(homes, left_on=team_col, right_on="team_id", how="inner")
        .filter(
            (pl.col("season") >= pl.col("first_season")) & (pl.col("season") <= pl.col("last_season").fill_null(9999))
        )
        .select("_r", pl.col("lat").alias("h_lat"), pl.col("lon").alias("h_lon"), pl.col("tz").alias("h_tz"))
    )
    out = indexed.join(hit, on="_r", how="left", validate="1:1").drop("_r")
    missing = out.filter(pl.col("h_lat").is_null())
    if missing.height:
        raise ValueError(
            f"teams with no game_sites.csv home row for the season: {missing.select(team_col, 'season').unique()}"
        )
    return out


def _place_games(games: pl.DataFrame, league_id: str) -> pl.DataFrame:
    """``games`` with the ``lat`` / ``lon`` / ``tz`` of where each was played.

    Raises:
        ValueError: If a home team has no ``game_sites.csv`` row for the season, or a neutral
            game's city has no site row.
    """
    sites = game_sites().filter(pl.col("league") == _LEAGUE[league_id])
    neutral = nba_neutral_site_games(league_id=league_id).select("game_id", pl.col("city").alias("place"))
    neutral = neutral.join(
        sites.filter(pl.col("kind") == "neutral").select("place", "lat", "lon", "tz"),
        on="place",
        how="left",
        validate="m:1",
    )
    if neutral.get_column("lat").null_count():
        raise ValueError(f"neutral cities with no game_sites.csv row: {neutral.filter(pl.col('lat').is_null())}")
    bubble = sites.filter(pl.col("kind") == "bubble").row(0, named=True)
    in_bubble = pl.col("game_date").is_between(*BUBBLES[league_id])
    return (
        _home_sites(games, "home_team_id", league_id)
        .join(neutral.drop("place"), on="game_id", how="left", validate="1:1")
        .select(
            *games.columns,
            *(
                pl.when(in_bubble).then(pl.lit(bubble[c])).otherwise(pl.coalesce(c, f"h_{c}")).alias(c)
                for c in ("lat", "lon", "tz")
            ),
        )
    )


def _haversine_km(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    p1, p2 = lat1.radians(), lat2.radians()
    a = ((p2 - p1) / 2).sin() ** 2 + p1.cos() * p2.cos() * ((lon2.radians() - lon1.radians()) / 2).sin() ** 2
    return 2 * _EARTH_KM * a.sqrt().arcsin()


def team_game_rest_travel(games: pl.DataFrame, league_id: str = "00") -> pl.DataFrame:
    """Rest and travel per team and game (the module doc's features).

    Args:
        games: Every game of the teams' seasons that sets their rest (regular season, and the NBA
            Cup final): ``season`` (Int64), ``game_id`` (Utf8), ``game_date`` (Date),
            ``home_team_id`` / ``away_team_id`` (Int64). Pass the whole schedule, played and
            unplayed: a game's features depend on the team's previous games only.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.

    Returns:
        One row per team and game in :data:`TEAM_GAME_SCHEMA`.

    Raises:
        ValueError: If ``league_id`` is unknown or a game cannot be placed (:func:`_place_games`).
    """
    if league_id not in _LEAGUE:
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    if games.height == 0:
        return pl.DataFrame(schema=TEAM_GAME_SCHEMA)
    placed = _place_games(games, league_id)
    sides = pl.concat(
        [
            placed.select("season", "game_id", "game_date", pl.col(f"{s}_team_id").alias("team_id"), "lat", "lon", "tz")
            for s in ("home", "away")
        ]
    )
    team = _home_sites(sides, "team_id", league_id).sort("team_id", "season", "game_date", "game_id")
    over = ["team_id", "season"]
    team = team.with_columns(
        pl.col("game_date").shift(1).over(over).alias("d1"),
        pl.col("game_date").shift(2).over(over).alias("d2"),
        pl.col("lat").shift(1).over(over).fill_null(pl.col("h_lat")).alias("p_lat"),
        pl.col("lon").shift(1).over(over).fill_null(pl.col("h_lon")).alias("p_lon"),
        pl.col("tz").shift(1).over(over).fill_null(pl.col("h_tz")).alias("p_tz"),
    )
    # both sites' clocks on this game's date: a team that stays put across a DST switch crosses none
    shift = [
        abs(_utc_offset_hours(tz, d) - _utc_offset_hours(ptz, d))
        for d, tz, ptz in team.select("game_date", "tz", "p_tz").iter_rows()
    ]
    rest = ((pl.col("game_date") - pl.col("d1")).dt.total_days() - 1).clip(0, REST_CAP).fill_null(REST_CAP)
    return team.select(
        "game_id",
        "team_id",
        rest.cast(pl.Int64).alias("rest_days"),
        (rest == 0).cast(pl.Int64).alias("b2b"),
        ((pl.col("game_date") - pl.col("d2")).dt.total_days() <= 3)
        .fill_null(False)
        .cast(pl.Int64)
        .alias("three_in_four"),
        _haversine_km(pl.col("p_lat"), pl.col("p_lon"), pl.col("lat"), pl.col("lon")).alias("travel_km"),
        pl.Series("tz_shift", shift, dtype=pl.Float64),
    )


def game_rest_travel(games: pl.DataFrame, league_id: str = "00") -> pl.DataFrame:
    """``games`` with the home-minus-away difference of each of :data:`FEATURES`.

    Args:
        games: As :func:`team_game_rest_travel`.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.

    Returns:
        ``games`` with ``d_b2b``, ``d_three_in_four``, ``d_rest_days``, ``d_travel`` (thousands
        of km) and ``d_tz_shift``.
    """
    feats = team_game_rest_travel(games, league_id).with_columns((pl.col("travel_km") / 1000.0).alias("travel"))
    out = games
    for side in ("home", "away"):
        assert out.schema[f"{side}_team_id"] == feats.schema["team_id"], "team_id dtype differs across the join"
        out = out.join(
            feats.select("game_id", "team_id", *(pl.col(f).alias(f"{side}_{f}") for f in FEATURES)),
            left_on=["game_id", f"{side}_team_id"],
            right_on=["game_id", "team_id"],
            how="left",
            validate="1:1",
        )
    return out.select(
        *games.columns,
        *((pl.col(f"home_{f}") - pl.col(f"away_{f}")).cast(pl.Float64).alias(f"d_{f}") for f in FEATURES),
    )


def rest_travel_margin(diffs: pl.DataFrame, league_id: str = "00", coefs: dict[str, float] | None = None) -> np.ndarray:
    """Points of expected home margin from :func:`game_rest_travel`'s differences.

    Args:
        diffs: Frame with the ``d_*`` columns of :func:`game_rest_travel`.
        league_id: Selects :data:`REST_TRAVEL_CONSTANTS`.
        coefs: Override the fitted constants (the fitting script).

    Returns:
        One value per row; zeros when the league has no fitted entry.
    """
    c = REST_TRAVEL_CONSTANTS.get(league_id) if coefs is None else coefs
    if not c:
        return np.zeros(diffs.height)
    out = np.zeros(diffs.height)
    for f in FEATURES:
        out = out + c[f] * diffs.get_column(f"d_{f}").fill_null(0.0).to_numpy()
    return out
