"""Our own WAR and VORP (BB-D26): the shared core behind ``{lg}_player_war``.

One row per ``(season, player_id)``, built from our own per-100-possession ratings: BPM, RAPM
(the prior-informed ``adj_rapm``) and BOBAN in the NBA and WNBA, box BPM in college. No scraped
third-party VORP is read.

**Team anchoring.** Each rating is re-anchored to the team, the way BPM 2.0 anchors its box
score: within a team-season, a player's rating minus his team's possession-weighted mean, plus
the team's net rating per 100 divided by 5. The five men on the floor then add up to the team's
net rating, so a team's points above average are exactly its point margin, whatever offset the
source carries (``o_adj_rapm`` / ``d_adj_rapm`` sit about +11 / -11, the ``adj_rapm`` sum is off
by about +1). A traded player is anchored on each team for the possessions he played there. The
ratings are then centred on the league: their possession-weighted mean is 0 each season. A
player missing a rating is his team's average on that team.

**Value.** With ``R`` the league's replacement level and ``poss`` his possessions:

* ``vorp = (rating - R) * poss / lg_team_poss``: points per 100 team possessions above a
  replacement player, over the possessions one team plays in the season (Basketball-Reference's
  VORP scale: playing every possession of a season at ``R + 1`` is a VORP of 1).
* ``war = (rating - R) * poss / 100 / pts_per_win``: wins above replacement.

``R`` (:data:`REPLACEMENT_LEVEL`) and ``pts_per_win`` (:data:`PTS_PER_WIN`) are per league,
fitted by ``tools/fit_player_war.py`` on train seasons and frozen. A replacement-level player
has ``war == 0``. Because the ratings are centred, a season's ``sum(war)`` is
``-R * sum(poss) / 100 / pts_per_win``: the league's wins minus the wins of a team of
replacement players (:func:`replacement_win_pct`).

**Replacement level** is the possession-weighted mean box-BPM rating of the players who,
together, played the league's least-used :data:`REPLACEMENT_SHARE` of the possessions (the
season's players sorted by possessions, fewest first). Box BPM, because RAPM and BOBAN pull
low-minute players toward the mean by design and would put the fringe near average. The level is
shared by every rating in the league, so the three pro WARs add up to the same league total. It
is a box-score level: RAPM and BOBAN shrink the fringe toward average, so under ``war_rapm`` and
above all ``war_boban`` the pool carries more WAR than under ``war_bpm`` (NBA 2024-25, a
season whose pool sits 0.8 above ``R``: 46 under ``war_bpm`` and ``war_rapm``, 90 under
``war_boban``, of 765).

**Uncertainty.** The pro rows carry ``war_bpm`` / ``war_rapm`` / ``war_boban`` beside the mean:
their spread is the model uncertainty. ``R`` is a mean over 5-8 train seasons, standard error
0.09 (NBA), 0.18 (WNBA), 0.06 (MBB), 0.11 (WBB) per 100; a shift of ``dR`` moves a player's WAR
by ``-dR * poss / 100 / pts_per_win`` (0.3 wins for an NBA starter per 0.1).

**Minutes basis.** ``poss`` is a player's on-court possessions, ``(offensive + defensive) / 2``.
NBA and WNBA count them from the stats.nba.com possessions (every possession row, BB-D3's
convention); college has no published on-court possessions for every game, so it is
``minutes / 40 * adj_tempo`` (the team's adjusted possessions per 40 minutes).

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes

__all__ = [
    "PTS_PER_WIN",
    "REPLACEMENT_LEVEL",
    "REPLACEMENT_SHARE",
    "player_war",
    "player_war_schema",
    "repair_bpm_team_constant",
    "replacement_level_by_season",
    "replacement_win_pct",
]

#: The replacement pool: the players who together played the league's least-used share of
#: possessions, the same in every league. A convention, picked among 10%, 15% and 20% by the NBA
#: level it gives over 1997-2026 against Basketball-Reference's -2.0 BPM (-2.61, -2.32, -2.08
#: then; -1.98 on the train span now). In college it is the deep bench of every D1 team, which
#: sits only about 1.7 per 100 below average: a team of them would win about .35 against an
#: average D1 schedule, and about a third of D1 teams rate below that.
REPLACEMENT_SHARE = 0.15

#: league -> replacement level ``R``, points per 100 possessions relative to league average, and
#: league -> points of season margin per win. Both fitted by ``tools/fit_player_war.py``
#: (2026-11-05) on the TRAIN seasons and scored on the later HOLDOUT (player-seasons in
#: ``tests/fixtures/bb_player_war/``, final scores in ``tests/fixtures/bb_team_luck/``). The pool
#: level drifts up as rosters deepen (NBA about -2.8 per 100 before 2010, about -1.9 since 2014),
#: so the train span is the stretch just before the holdout: ``R`` describes today's league, and
#: WAR before about 2010 is conservative. The drift continues inside the holdout in college:
#: the season levels sit above ``R`` by +0.23 (MBB, 6 of 7 seasons) and +0.42 (WBB, all 5) on
#: average, so current college league WAR runs about 15% (MBB) and 25% (WBB) above the season's
#: own pool. Refit every year after the season (move ``SPANS`` forward a season). Points per win
#: is fitted through the origin; in college the D1 teams' wins over non-D1 opponents bias it
#: about 1.5% high (38.35 / 46.94 with an intercept). The replacement team's win percentage is
#: :func:`replacement_win_pct` at the 2024-25 / 2025 possessions per game (college: against an
#: average D1 schedule).
#:
#: ====== ===== ======= ========= ========= ========================= ============ ===========
#: league R     pts/win train     holdout   season level - R: |mean|, holdout MAE  replacement
#:                                          signed mean, max          (wins)       team, 2025
#: ====== ===== ======= ========= ========= ========================= ============ ===========
#: nba    -1.98 31.56   2012-2019 2020-2026 0.193, +0.107, 0.794 2025 2.717 (210)  .189
#: wnba   -3.18 31.31   2010-2017 2018-2025 0.633, +0.182, 1.259 2025 1.400 (97)   .104
#: mbb    -1.66 38.91   2014-2019 2020-2026 0.316, +0.234, 0.463      1.684 (2493) .353
#: wbb    -1.99 47.10   2017-2021 2022-2026 0.417, +0.417, 0.565      1.675 (1802) .350
#: ====== ===== ======= ========= ========= ========================= ============ ===========
REPLACEMENT_LEVEL: dict[str, float] = {"nba": -1.98, "wnba": -3.18, "mbb": -1.66, "wbb": -1.99}
PTS_PER_WIN: dict[str, float] = {"nba": 31.56, "wnba": 31.31, "mbb": 38.91, "wbb": 47.10}


def player_war_schema(player_dtype: pl.DataType, team_dtype: pl.DataType, sources: list[str]) -> dict[str, pl.DataType]:
    """Columns of :func:`player_war` for a league's id dtypes and rating ``sources``.

    With one source (college) the per-source ``rating_<s>`` / ``war_<s>`` columns are left out:
    ``rating`` and ``war`` are that source.
    """
    per = sources if len(sources) > 1 else []
    return {
        "season": pl.Int64(),
        "player_id": player_dtype,
        "player_name": pl.Utf8(),
        "team_id": team_dtype,
        "teams": pl.Int64(),
        "min": pl.Float64(),
        "poss": pl.Float64(),
        "rating": pl.Float64(),
        **{f"rating_{s}": pl.Float64() for s in per},
        "vorp": pl.Float64(),
        "war": pl.Float64(),
        **{f"war_{s}": pl.Float64() for s in per},
        "vorp_rank": pl.Float64(),
        "war_rank": pl.Float64(),
        "vorp_pct": pl.Float64(),
        "war_pct": pl.Float64(),
        "vorp_n": pl.Int64(),
        "war_n": pl.Int64(),
    }


def _wmean(col: str, weight: str, by: list[str]) -> pl.Expr:
    """``weight``-weighted mean of ``col`` within ``by``, over the rows where ``col`` is set."""
    w = pl.when(pl.col(col).is_not_null()).then(pl.col(weight))
    return (pl.col(col) * pl.col(weight)).sum().over(by) / w.sum().over(by)


def repair_bpm_team_constant(ratings: pl.DataFrame, net: pl.DataFrame) -> pl.DataFrame:
    """Give the published pro BPM its BPM 2.0 team constant: minute-weighted team mean = net / 5.

    ``nba_bpm`` (the ``bpm`` of ``nba_player_impact`` / ``wnba_player_impact``) adds a per-team
    constant so the minute-weighted MEAN of a team's BPMs equals its net rating per 100, where BPM
    2.0 has the minute-weighted SUM over five slots (the mean times 5) equal it. Every player on a
    team is off by 0.8 times the team's net rating (team sums run about 5x the margin), and the
    constant goes to ``obpm`` only, which is why every ``dbpm`` is positive. This re-centres
    ``bpm`` on its team as listed in the release; it is idempotent, so it stays right once the
    producer is fixed. ``obpm`` / ``dbpm`` are not used.

    Args:
        ratings: ``season``, ``team_id``, ``min`` and ``bpm``, one row per player-season.
        net: ``season``, ``team_id`` and ``net`` (the team's net rating per 100).

    Returns:
        ``ratings`` with ``bpm`` repaired (null where the team has no ``net``).
    """
    keys = ["season", "team_id"]
    assert all(ratings.schema[k] == net.schema[k] for k in keys), "key dtypes differ between ratings and net"
    return (
        ratings.join(net.select(*keys, pl.col("net").alias("_net")), on=keys, how="left", validate="m:1")
        .with_columns((pl.col("bpm") - _wmean("bpm", "min", keys) + pl.col("_net") / 5).alias("bpm"))
        .drop("_net")
    )


def _anchor(stints: pl.DataFrame, net: pl.DataFrame, sources: list[str]) -> pl.DataFrame:
    """Team-anchor every source on each stint, then centre it on the league (possession-weighted)."""
    keys = ["season", "team_id"]
    assert all(stints.schema[k] == net.schema[k] for k in keys), "key dtypes differ between stints and net"
    out = stints.join(net.select(*keys, "net"), on=keys, how="inner", validate="m:1")
    out = out.with_columns(
        ((pl.col(s) - _wmean(s, "poss", keys)).fill_null(0.0) + pl.col("net") / 5).alias(s) for s in sources
    )
    return out.with_columns((pl.col(s) - _wmean(s, "poss", ["season"])).alias(s) for s in sources)


#: The season's possessions per team: on-court possessions / 5 / teams.
_LG_TEAM_POSS = pl.col("poss").sum().over("season") / 5 / pl.col("team_id").n_unique().over("season")


def player_war(
    stints: pl.DataFrame,
    net: pl.DataFrame,
    *,
    league: str,
    sources: list[str],
) -> pl.DataFrame:
    """WAR and VORP per player-season from rating stints.

    Args:
        stints: One row per ``(season, team_id, player_id)``: ``player_name``, ``min``, ``poss``
            (on-court possessions with that team) and one column per name in ``sources`` (the
            player's season rating per 100, null when he has none).
        net: One row per ``(season, team_id)`` with ``net``: the team's net rating per 100 the
            ratings are anchored to (actual in the pros, D1-centred adjusted in college).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (picks the fitted constants).
        sources: The rating columns; ``rating`` / ``war`` are their mean.

    Returns:
        :func:`player_war_schema` rows sorted by season and ``war`` (most first). ``team_id`` is
        the team he played most possessions for and ``teams`` how many he played for. ``war`` and
        ``vorp`` carry ``_rank`` (1 = most), ``_pct`` and ``_n`` (possessions) among every
        player of the season, null under :data:`MIN_COHORT_PLAYERS`. Stints on a team without
        ``net`` are dropped. Empty input returns that schema with zero rows.
    """
    schema = player_war_schema(stints.schema["player_id"], stints.schema["team_id"], sources)
    if stints.height == 0:
        return pl.DataFrame(schema=schema)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    a = _anchor(stints, net, sources).with_columns(
        pl.mean_horizontal(sources).alias("rating"), _LG_TEAM_POSS.alias("_lg_poss")
    )
    rated = ["rating", *sources] if len(sources) > 1 else ["rating"]
    a = a.with_columns(((pl.col(s) - repl) * pl.col("poss")).alias(f"_par_{s}") for s in rated)
    out = (
        a.sort("poss", descending=True)
        .group_by("season", "player_id", maintain_order=True)
        .agg(
            pl.col("player_name").drop_nulls().first(),
            pl.col("team_id").first(),
            pl.col("team_id").n_unique().cast(pl.Int64).alias("teams"),
            pl.col("min").sum(),
            pl.col("poss").sum(),
            pl.col("_lg_poss").first(),
            *[(pl.col(s) * pl.col("poss")).sum().alias(f"_rp_{s}") for s in rated],
            *[pl.col(f"_par_{s}").sum() for s in rated],
        )
        .with_columns(
            *[(pl.col(f"_rp_{s}") / pl.col("poss")).alias("rating" if s == "rating" else f"rating_{s}") for s in rated],
            (pl.col("_par_rating") / pl.col("_lg_poss")).alias("vorp"),
            *[(pl.col(f"_par_{s}") / 100 / ppw).alias("war" if s == "rating" else f"war_{s}") for s in rated],
        )
    )
    return (
        out.pipe(attach_ranks, ["vorp", "war"], min_cohort=MIN_COHORT_PLAYERS, by=("season",))
        .pipe(attach_sample_sizes, {"vorp": pl.col("poss"), "war": pl.col("poss")})
        .select(list(schema))
        .cast(schema)
        .sort(["season", "war", "player_id"], descending=[False, True, False], nulls_last=True)
    )


def replacement_level_by_season(
    player_seasons: pl.DataFrame, *, share: float = REPLACEMENT_SHARE, rating: str = "rating_bpm"
) -> pl.DataFrame:
    """The replacement pool's possession-weighted mean rating, per season.

    Args:
        player_seasons: ``season``, ``poss`` and the anchored box rating, one row per
            player-season (:func:`player_war` output).
        share: The pool: the least-used players whose possessions add up to at most this share of
            the season's.
        rating: The box rating column: ``rating_bpm`` in the pros, ``rating`` in college.

    Returns:
        One row per season: ``replacement_level``, ``pool_players`` and ``pool_share`` (the share
        of possessions the pool played).
    """
    ranked = player_seasons.filter(pl.col("poss") > 0).sort(["season", "poss", rating])
    ranked = ranked.with_columns(
        (pl.col("poss").cum_sum().over("season") / pl.col("poss").sum().over("season")).alias("_cum")
    )
    return (
        ranked.filter(pl.col("_cum") <= share)
        .group_by("season")
        .agg(
            ((pl.col(rating) * pl.col("poss")).sum() / pl.col("poss").sum()).alias("replacement_level"),
            pl.len().alias("pool_players"),
            pl.col("_cum").max().alias("pool_share"),
        )
        .sort("season")
    )


def replacement_win_pct(league: str, poss_per_game: float) -> float:
    """Win percentage of a team of five replacement players: ``0.5 + 5 * R * poss_per_game / 100 / pts_per_win``."""
    return 0.5 + 5 * REPLACEMENT_LEVEL[league] * poss_per_game / 100 / PTS_PER_WIN[league]
