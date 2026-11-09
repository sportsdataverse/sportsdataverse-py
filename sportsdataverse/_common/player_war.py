"""Our own WAR and VORP (BB-D26): the shared core behind ``{lg}_player_war``.

One row per ``(season, player_id)``, built from our own per-100-possession ratings: BPM, RAPM
(the prior-informed ``adj_rapm``) and BOBAN in the NBA and WNBA, box BPM in college. No scraped
third-party VORP is read.

**Team anchoring.** Each rating is re-anchored to the team, the way BPM 2.0 anchors its box
score: within a team-season, a player's rating minus his team's possession-weighted mean, plus
the team's net rating per 100 divided by 5. Over the season the team's possession-weighted mean
rating is then its net rating / 5 (before the league centring, which shifts every team alike), so
a team's points above average are exactly its point margin, whatever offset the
source carries (in releases fit before sportsdataverse-py #1137 ``o_adj_rapm`` / ``d_adj_rapm`` sit about
+11 / -11 and the ``adj_rapm`` sum is off by about +1). A traded player is anchored on each team for the possessions he played there. The
ratings are then centred on the league: their possession-weighted mean is 0 each season. A
player missing a rating (null, NaN or infinite) is his team's average on that team.

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

**Uncertainty.** ``war_lo`` / ``war_hi`` are a nominal 90% interval (5th and 95th percentiles)
from a cluster bootstrap: :data:`N_BOOT` replicates, each team-season's games resampled with
replacement (as many as it played), so a player's possessions in a game, his team's possessions
and its margin move together. Each team-season draws from its own generator, seeded on (seed,
season, team id), and seasons run one at a time, so an interval does not depend on what else is
in the call; Monte Carlo error at 1000 replicates is about 3% of the width (measured 2026-11-05
on the 2024-25 NBA test fixture, seed 0 against seed 1: median 0.017 wins, at most 0.25). Coverage is not checked. Each replicate recomputes his possessions, the team's net rating
(in college: the adjusted rating plus the replicate's change in the raw one), the team anchoring
and the league centring, holding the source ratings and ``R`` fixed. It is the sampling
uncertainty of the team side and the playing time, not of the source ratings themselves: the pro
rows carry ``war_bpm`` / ``war_rapm`` / ``war_boban`` beside the mean, and their spread is that
model uncertainty. College has one source, so box BPM's own sampling error is in no column.
``vorp`` has the same interval scaled by ``vorp / war`` (the two differ by a season constant).
``R`` is a mean over 5-8 train seasons, standard error
0.09 (NBA), 0.18 (WNBA), 0.06 (MBB), 0.11 (WBB) per 100; a shift of ``dR`` moves a player's WAR
by ``-dR * poss / 100 / pts_per_win`` (about 0.17 wins per 0.1 for an NBA starter with 5,300 on-court possessions).

**Minutes basis.** ``poss`` is a player's on-court possessions, ``(offensive + defensive) / 2``.
NBA and WNBA count them from the stats.nba.com possessions (every possession row, BB-D3's
convention); college has no published on-court possessions for every game, so it is
``minutes / 40 * adj_tempo`` (the team's adjusted possessions per 40 minutes).

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import zlib

import numpy as np
import polars as pl
from scipy import sparse

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes

__all__ = [
    "N_BOOT",
    "PTS_PER_WIN",
    "REPLACEMENT_LEVEL",
    "REPLACEMENT_SHARE",
    "player_war",
    "player_war_schema",
    "repair_bpm_team_constant",
    "replacement_level_by_season",
    "replacement_win_pct",
    "season_rows",
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

#: Bootstrap replicates behind ``war_lo`` / ``war_hi``, and the interval's percentiles.
N_BOOT = 1000
_INTERVAL = (5.0, 95.0)
_SEED = 0


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
        "war_lo": pl.Float64(),
        "war_hi": pl.Float64(),
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
    constant goes to ``obpm`` only, which is why every ``dbpm`` is positive (releases fit before
    sportsdataverse-py #1148; ``nba_bpm`` now applies the BPM 2.0 constants itself). This
    re-centres ``bpm`` on its team as listed in the release; it is idempotent, so on a rebuilt
    release it moves ``bpm`` only by the gap between the possession and box net ratings.
    ``obpm`` / ``dbpm`` are not used.

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
    # a NaN or infinite rating would make its team's and season's means non-finite: treat it as missing
    finite = stints.with_columns(pl.when(pl.col(s).is_finite()).then(pl.col(s)).alias(s) for s in sources)
    out = finite.join(net.select(*keys, "net"), on=keys, how="inner", validate="m:1")
    out = out.with_columns(
        ((pl.col(s) - _wmean(s, "poss", keys)).fill_null(0.0) + pl.col("net") / 5).alias(s) for s in sources
    )
    return out.with_columns((pl.col(s) - _wmean(s, "poss", ["season"])).alias(s) for s in sources)


def season_rows(player_games: pl.DataFrame, team_games: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Sum game rows to the season: player possessions per team, and each team's net rating.

    Args:
        player_games: ``season``, ``team_id``, ``game_id``, ``player_id`` and ``poss`` (on-court
            possessions in that game).
        team_games: ``season``, ``team_id``, ``game_id``, ``poss`` (the team's) and ``margin``
            (points for minus against).

    Returns:
        ``(stints, net)``: one row per ``(season, team_id, player_id)`` with ``poss``, and one
        per ``(season, team_id)`` with ``poss`` and ``net`` (points per 100 minus its opponents').
    """
    stints = player_games.group_by("season", "team_id", "player_id").agg(pl.col("poss").sum())
    net = team_games.group_by("season", "team_id").agg(pl.col("poss").sum(), pl.col("margin").sum())
    return stints, net.select("season", "team_id", "poss", (pl.col("margin") / pl.col("poss") * 100).alias("net"))


def _member(rows: np.ndarray, n: int) -> sparse.csr_matrix:
    """A rows-by-``n`` 0/1 membership matrix: row ``i`` belongs to group ``rows[i]``."""
    return sparse.csr_matrix((np.ones(len(rows)), (np.arange(len(rows)), rows)), shape=(len(rows), n))


def _replicate_war(
    frames: dict[str, pl.DataFrame], counts: np.ndarray, *, league: str, sources: list[str]
) -> np.ndarray:
    """WAR per player-season under each row of ``counts`` (replicates by team-games), as :func:`player_war`.

    ``frames`` are :func:`_boot_frames`; ``counts[b, g]`` is how often team-game ``g`` is drawn in
    replicate ``b``. All ones reproduces :func:`player_war`'s ``war``.
    """
    st, tg, pg = frames["stints"], frames["team_games"], frames["player_games"]
    n_s, n_t, n_g = st.height, frames["n_teams"], tg.height
    occ = sparse.csr_matrix((pg["poss"].to_numpy(), (pg["s"].to_numpy(), pg["g"].to_numpy())), shape=(n_s, n_g))
    poss = (occ @ counts.T).T  # replicate x stint
    g_team = _member(tg["t"].to_numpy(), n_t).T  # team x game
    t_poss = (g_team @ (counts * tg["poss"].to_numpy()).T).T
    t_margin = (g_team @ (counts * tg["margin"].to_numpy()).T).T
    t_idx = tg["t"].to_numpy()
    # a rated team with no stints and no games (a D1 team with no box rows) is 0 / 0 and touches no player
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = np.bincount(t_idx, tg["margin"].to_numpy(), n_t) / np.bincount(t_idx, tg["poss"].to_numpy(), n_t)
        net = frames["net"] + (t_margin / t_poss - raw) * 100
    t_of, z_of = st["t"].to_numpy(), st["z"].to_numpy()
    team, season = _member(t_of, n_t), _member(z_of, int(z_of.max()) + 1)
    rating = np.zeros_like(poss)
    for src in sources:
        r = st[src].to_numpy().astype(float)
        ok = np.isfinite(r)
        w = poss * ok
        mean = (team.T @ (w * np.where(ok, r, 0.0)).T).T / np.where((den := (team.T @ w.T).T) > 0, den, 1.0)
        a = np.where(ok, r - mean[:, t_of], 0.0) + net[:, t_of] / 5
        a -= ((season.T @ (a * poss).T) / (season.T @ poss.T)).T[:, z_of]
        rating += a / len(sources)
    par = (rating - REPLACEMENT_LEVEL[league]) * poss
    return (_member(st["p"].to_numpy(), frames["n_players"]).T @ par.T).T / 100 / PTS_PER_WIN[league]


def _boot_frames(stints: pl.DataFrame, net: pl.DataFrame, team_games: pl.DataFrame, player_games: pl.DataFrame) -> dict:
    """Integer-index the stints, teams, seasons, player-seasons and team-games for the bootstrap."""
    keys = ["season", "team_id"]
    teams = net.select(*keys, "net").sort(keys).with_row_index("t")
    st = stints.join(teams, on=keys, how="inner", validate="m:1").with_row_index("s")
    players = st.select("season", "player_id").unique().sort("season", "player_id").with_row_index("p")
    st = st.join(players, on=["season", "player_id"], how="left", validate="m:1").sort("s")
    st = st.with_columns(pl.col("season").rank("dense").cast(pl.Int64).sub(1).alias("z"))
    # sorted, so a fixed seed gives the same draws whatever order the game rows came in
    tg = team_games.join(teams.select(*keys, "t"), on=keys, how="inner").sort(*keys, "game_id").with_row_index("g")
    pg = player_games.join(st.select(*keys, "player_id", "s"), on=[*keys, "player_id"], how="inner").join(
        tg.select(*keys, "game_id", "g"), on=[*keys, "game_id"], how="inner"
    )
    return {
        "stints": st,
        "team_games": tg,
        "player_games": pg,
        "n_teams": teams.height,
        "n_players": players.height,
        "net": teams["net"].to_numpy(),
    }


def _draw_counts(team_games: pl.DataFrame, n_boot: int) -> np.ndarray:
    """``counts[b, g]``: how often team-game ``g`` is drawn in replicate ``b``.

    Each team-season (``t``) draws as many of its own games as it played, with replacement, so
    its game count is fixed and a game's possessions and margin move together. Each team-season
    has its own generator, seeded on (seed, season, team id), so its draws do not depend on which
    other teams or seasons are in the call.
    """
    counts = np.zeros((n_boot, team_games.height))
    groups = team_games.group_by("season", "team_id", "t", maintain_order=True).agg("g")
    for season, team, _, idx in groups.iter_rows():
        rng = np.random.default_rng([_SEED, int(season), zlib.crc32(str(team).encode())])
        counts[:, idx] = rng.multinomial(len(idx), np.full(len(idx), 1 / len(idx)), size=n_boot)
    return counts


def war_interval(
    stints: pl.DataFrame,
    net: pl.DataFrame,
    team_games: pl.DataFrame,
    player_games: pl.DataFrame,
    *,
    league: str,
    sources: list[str],
    n_boot: int = N_BOOT,
) -> pl.DataFrame:
    """``war_lo`` / ``war_hi`` per player-season: a cluster bootstrap over each team-season's games.

    Each replicate draws, for every team-season, as many of its games as it played, with
    replacement, and recomputes :func:`player_war`'s WAR from them (possessions, team net rating,
    anchoring, centring; the source ratings and ``R`` held fixed). Seasons run one at a time.

    Returns:
        ``season``, ``player_id``, ``war_lo`` and ``war_hi`` (the 5th and 95th percentiles).

    Raises:
        ValueError: If a stint's possessions are not the sum of its game rows: the bootstrap
            would resample possessions the point estimate never counted (or miss some).
    """
    keys = ["season", "team_id", "player_id"]
    summed = player_games.group_by(keys).agg(pl.col("poss").sum().alias("_games"))
    check = stints.join(net.select("season", "team_id"), on=keys[:2], how="semi").join(summed, on=keys, how="left")
    gap = (pl.col("_games").fill_null(0.0) - pl.col("poss")).abs() > 1e-6 * pl.max_horizontal(pl.col("poss"), 1.0)
    if (bad := check.filter(gap)).height:
        raise ValueError(f"war_interval: {bad.height} stints' possessions differ from their game rows")
    out = []
    for season in sorted(stints["season"].unique()):
        in_season = [df.filter(pl.col("season") == season) for df in (stints, net, team_games, player_games)]
        frames = _boot_frames(*in_season)
        draws = _replicate_war(frames, _draw_counts(frames["team_games"], n_boot), league=league, sources=sources)
        lo, hi = np.percentile(draws, _INTERVAL, axis=0)
        players = frames["stints"].group_by("p").agg(pl.col("season").first(), pl.col("player_id").first()).sort("p")
        assert players.height == frames["n_players"]
        out.append(players.select("season", "player_id").with_columns(pl.Series("war_lo", lo), pl.Series("war_hi", hi)))
    return pl.concat(out)


#: The season's possessions per team: on-court possessions / 5 / teams.
_LG_TEAM_POSS = pl.col("poss").sum().over("season") / 5 / pl.col("team_id").n_unique().over("season")


def player_war(
    stints: pl.DataFrame,
    net: pl.DataFrame,
    *,
    league: str,
    sources: list[str],
    team_games: pl.DataFrame | None = None,
    player_games: pl.DataFrame | None = None,
    n_boot: int = N_BOOT,
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
        team_games: The games behind ``net`` (:func:`season_rows`), for the interval.
        player_games: The games behind ``stints``' ``poss`` (:func:`season_rows`), for the
            interval. Without both, ``war_lo`` / ``war_hi`` are null; the ``{lg}_player_war``
            functions always pass them.
        n_boot: Bootstrap replicates behind ``war_lo`` / ``war_hi`` (:func:`war_interval`).

    Returns:
        :func:`player_war_schema` rows sorted by season and ``war`` (most first). ``team_id`` is
        the team he played most possessions for and ``teams`` how many he played for. ``war`` and
        ``vorp`` carry ``_rank`` (1 = most), ``_pct`` and ``_n`` (possessions) among every
        player of the season, null under :data:`MIN_COHORT_PLAYERS`; ``war_lo`` / ``war_hi`` are
        the 90% interval of :func:`war_interval`. Stints on a team without ``net`` are dropped.
        Empty input returns that schema with zero rows.
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
    if team_games is not None and player_games is not None:
        interval = war_interval(stints, net, team_games, player_games, league=league, sources=sources, n_boot=n_boot)
        assert out.schema["player_id"] == interval.schema["player_id"], "player_id dtype differs in the interval"
        out = out.join(interval, on=["season", "player_id"], how="left", validate="1:1")
    else:
        out = out.with_columns(pl.lit(None, pl.Float64).alias("war_lo"), pl.lit(None, pl.Float64).alias("war_hi"))
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
