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
  replacement player, over the possessions one team plays in the season (so playing every
  possession of a season at ``R + 1`` per 100 is a VORP of 1).
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

**Per game.** ``pts_pg`` / ``reb_pg`` / ``ast_pg`` are his box points, rebounds and assists over the
games he played (minutes over 0: the stats.nba.com regular-season game logs in the pros, the same ESPN box
rows the college ratings sum). Each carries ``_rank`` (1 = most), ``_pct`` and ``_n`` (games) among the
season's players with at least the games gate of
:data:`~sportsdataverse._common.rate_gates.PLAYER_BOX_MIN_N` (one game in the pros, one or two in college:
the split-half reliability of a per-game line passes 0.5 within two games). A player with no box line has
null values and ``_n`` 0.

**Tiers.** ``war_tier`` places a player on a five-step depth ladder by his WAR percentile within the
league-season's *tier cohort* (:func:`war_tiers`, :data:`WAR_TIERS`): ``Star`` 95 or more (about the top
5%), ``Starter`` 80 to under 95 (the next 15%), ``Rotation`` 50 to under 80 (30%), ``Bench`` 20 to under 50
(30%) and ``Fringe`` under 20 (the last 20%), each share to within a player at its floors. The tier cohort
is the players on the floor for at least :data:`TIER_MIN_POSS_SHARE` (a tenth) of the season's
possessions per team. WAR counts playing time, so without the floor a deep call-up with a few possessions
and a WAR near 0 would rank above a rotation player with negative WAR and fill the middle tiers. A player
under the floor has no tier, as does every player of a season whose cohort is under
:data:`MIN_COHORT_PLAYERS`. The percentile is the ``_pct`` formula, ``100 * (n + 1 - rank) / (n + 1)``
among the cohort, not ``war_pct`` (which ranks every player). College rows also carry ``war_tier_conf``:
the same ladder within the league-season's conference (the conference of the team he played most
possessions for), over the same possession floor; on another conference's team :func:`roster_tiers` places
him in that conference.

**Rosters.** ``team_ids`` lists every team a player played for, most possessions first.
:func:`roster_tiers` puts him on each of them with his full-season tiers, and :func:`roster_tier_shape`
gives the median roster from those rows: per tier, the median number of players per team, league-wide
(``scope`` ``league``) and in college per conference (``scope`` ``conference``). A traded player counts
on every team he played for, so a team's counts add up to every tiered player it used and the league's
rosters add up to more players than the league has.

**Cohort counts.** :func:`player_war_league_averages` gives the league-average rows of a season, whose
``n`` is the number of players each column is ranked among (the ``n`` of the ``_pct`` formula): the
count a rank is out of.

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

from sportsdataverse._common.league_context import (
    MIN_COHORT_PLAYERS,
    attach_ranks,
    attach_sample_sizes,
    league_averages,
)
from sportsdataverse._common.rate_gates import player_box_gates

__all__ = [
    "N_BOOT",
    "PER_GAME",
    "PTS_PER_WIN",
    "REPLACEMENT_LEVEL",
    "REPLACEMENT_SHARE",
    "TIER_MIN_POSS_SHARE",
    "TIER_SHAPE_SCHEMA",
    "WAR_TIERS",
    "box_seasons",
    "player_war",
    "player_war_league_averages",
    "player_war_schema",
    "repair_bpm_team_constant",
    "replacement_level_by_season",
    "replacement_win_pct",
    "roster_tier_shape",
    "roster_tiers",
    "season_rows",
    "war_tiers",
]

#: The replacement pool: the players who together played the league's least-used share of
#: possessions, the same in every league. A convention, picked among 10%, 15% and 20% by the NBA
#: level it gives over 1997-2026 against a -2.0 per 100 replacement level (-2.61, -2.32, -2.08
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

#: Per-game column -> the box count it averages over the games he played.
PER_GAME: dict[str, str] = {"pts_pg": "pts", "reb_pg": "reb", "ast_pg": "ast"}

#: The depth ladder, best first: tier -> the lowest tier-cohort WAR percentile it takes (a player is in the
#: first tier he clears; a floor is inclusive). Fixed floors, not fitted: over a league-season they hold
#: about the top 5%, then 15%, 30%, 30% and the last 20% (to within a player at each floor; a cohort of ten
#: has no Star, its best percentile being 100 * 10 / 11), a ladder that reads the same in every league and
#: season.
WAR_TIERS: dict[str, float] = {"Star": 95.0, "Starter": 80.0, "Rotation": 50.0, "Bench": 20.0, "Fringe": 0.0}

#: The tier cohort's floor: on-court possessions of at least this share of the season's possessions per
#: team (on-court possessions / 5 / teams), about 4.8 minutes a game over a full season. A share rather
#: than a count, so the live season's cohort grows with it from opening night. A convention, not fitted:
#: it keeps every regular rotation player and leaves out the call-ups whose WAR is near 0 because they
#: barely played (2024-25 fixtures: NBA 400 of 569 players at 812 on-court possessions or more, MBB 3,714 of
#: 5,130 at 227).
TIER_MIN_POSS_SHARE = 0.10

#: Bootstrap replicates behind ``war_lo`` / ``war_hi``, and the interval's percentiles.
N_BOOT = 1000
_INTERVAL = (5.0, 95.0)
_SEED = 0


def player_war_schema(
    player_dtype: pl.DataType, team_dtype: pl.DataType, sources: list[str], *, conferences: bool = False
) -> dict[str, pl.DataType]:
    """Columns of :func:`player_war` for a league's id dtypes and rating ``sources``.

    With one source (college) the per-source ``rating_<s>`` / ``war_<s>`` columns are left out:
    ``rating`` and ``war`` are that source. ``conferences`` (college) adds ``conference_id`` and
    ``war_tier_conf``.
    """
    per = sources if len(sources) > 1 else []
    conf = {"conference_id": pl.Utf8()} if conferences else {}
    return {
        "season": pl.Int64(),
        "player_id": player_dtype,
        "player_name": pl.Utf8(),
        "team_id": team_dtype,
        "teams": pl.Int64(),
        "team_ids": pl.Utf8(),
        **conf,
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
        "war_tier": pl.Utf8(),
        **({"war_tier_conf": pl.Utf8()} if conferences else {}),
        **{m: pl.Float64() for m in PER_GAME},
        **{f"{m}_rank": pl.Float64() for m in PER_GAME},
        **{f"{m}_pct": pl.Float64() for m in PER_GAME},
        **{f"{m}_n": pl.Int64() for m in PER_GAME},
    }


def box_seasons(rows: pl.DataFrame) -> pl.DataFrame:
    """Sum per-game box lines to the player-season: ``games``, ``pts``, ``reb`` and ``ast``.

    Args:
        rows: One row per game he played: ``season``, ``player_id``, ``pts``, ``reb``, ``ast``.

    Returns:
        One row per ``(season, player_id)`` with ``games`` (Int64) and the three totals (Float64).
    """
    return rows.group_by("season", "player_id").agg(
        pl.len().cast(pl.Int64).alias("games"), *[pl.col(c).cast(pl.Float64).sum() for c in PER_GAME.values()]
    )


def _attach_per_game(out: pl.DataFrame, box: pl.DataFrame | None, league: str) -> pl.DataFrame:
    """``out`` with :data:`PER_GAME` columns, ranked among the season's players over the games gate."""
    if box is None:
        cols = {"season": pl.Int64(), "player_id": out.schema["player_id"], "games": pl.Int64()}
        box = pl.DataFrame(schema={**cols, **{c: pl.Float64() for c in PER_GAME.values()}})
    assert out.schema["player_id"] == box.schema["player_id"], "player_id dtype differs between WAR and box"
    b = box.select(
        pl.col("season").cast(pl.Int64),
        "player_id",
        pl.col("games").alias("_games"),
        *[(pl.col(c) / pl.col("games")).alias(m) for m, c in PER_GAME.items()],
    )
    joined = out.join(b, on=["season", "player_id"], how="left", validate="1:1")
    sized = attach_sample_sizes(joined, {m: pl.col("_games") for m in PER_GAME})
    gates = player_box_gates(league, PER_GAME)
    return attach_ranks(sized, list(PER_GAME), min_cohort=MIN_COHORT_PLAYERS, by=("season",), min_n=gates).drop(
        "_games"
    )


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
    constant goes to ``obpm`` only, which leaves ``dbpm`` raw and positive for nearly every
    regular-season player (releases fit before
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
    box: pl.DataFrame | None = None,
    conferences: pl.DataFrame | None = None,
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
        box: :func:`box_seasons` rows for the per-game columns. Without it they are null (``_n`` 0);
            the ``{lg}_player_war`` functions always pass it.
        conferences: College only: ``season``, ``team_id`` and ``conference_id`` (Utf8) per team-season.
            Adds ``conference_id`` (his ``team_id``'s) and ``war_tier_conf`` (:func:`war_tiers`).

    Returns:
        :func:`player_war_schema` rows sorted by season and ``war`` (most first). ``team_id`` is
        the team he played most possessions for, ``teams`` how many he played for and ``team_ids``
        their ids, comma-separated, most possessions first. ``war`` and
        ``vorp`` carry ``_rank`` (1 = most), ``_pct`` and ``_n`` (possessions) among every
        player of the season, null under :data:`MIN_COHORT_PLAYERS`; ``war_tier`` is his
        :data:`WAR_TIERS` step in the tier cohort (:func:`war_tiers`; null under its floor); ``war_lo`` / ``war_hi`` are
        the 90% interval of :func:`war_interval`. ``pts_pg`` / ``reb_pg`` / ``ast_pg`` (module
        docstring) carry ``_rank`` (1 = most), ``_pct`` and ``_n`` (games) among the players over the
        games gate. Stints on a team without ``net`` are dropped.
        Empty input returns that schema with zero rows.
    """
    schema = player_war_schema(
        stints.schema["player_id"], stints.schema["team_id"], sources, conferences=conferences is not None
    )
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
            pl.col("team_id").cast(pl.Utf8).str.join(",").alias("team_ids"),
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
    if conferences is not None:
        conf = conferences.select("season", "team_id", pl.col("conference_id").cast(pl.Utf8)).unique(
            ["season", "team_id"]
        )
        assert out.schema["team_id"] == conf.schema["team_id"], "team_id dtype differs between WAR and conferences"
        out = out.join(conf, on=["season", "team_id"], how="left", validate="m:1")
    if team_games is not None and player_games is not None:
        interval = war_interval(stints, net, team_games, player_games, league=league, sources=sources, n_boot=n_boot)
        assert out.schema["player_id"] == interval.schema["player_id"], "player_id dtype differs in the interval"
        out = out.join(interval, on=["season", "player_id"], how="left", validate="1:1")
    else:
        out = out.with_columns(pl.lit(None, pl.Float64).alias("war_lo"), pl.lit(None, pl.Float64).alias("war_hi"))
    return (
        out.pipe(attach_ranks, ["vorp", "war"], min_cohort=MIN_COHORT_PLAYERS, by=("season",))
        .pipe(attach_sample_sizes, {"vorp": pl.col("poss"), "war": pl.col("poss")})
        .pipe(_attach_per_game, box, league)
        .pipe(war_tiers)
        .select(list(schema))
        .cast(schema)
        .sort(["season", "war", "player_id"], descending=[False, True, False], nulls_last=True)
    )


def _war_tier(pct: pl.Expr) -> pl.Expr:
    """``war_tier``: the first :data:`WAR_TIERS` step whose (inclusive) floor ``pct`` clears; null when it is null."""
    expr = pl.when(pct.is_null()).then(pl.lit(None, pl.Utf8))
    for tier, floor in WAR_TIERS.items():
        expr = expr.when(pct >= floor).then(pl.lit(tier))
    return expr.alias("war_tier")


def war_tiers(war: pl.DataFrame) -> pl.DataFrame:
    """``war`` with ``war_tier``: each player's :data:`WAR_TIERS` step within his season's tier cohort.

    The cohort is the season's players with ``poss`` at least :data:`TIER_MIN_POSS_SHARE` of the season's
    possessions per team: :func:`player_war`'s own season count, taken over the stints before each player is
    put on one team; standalone, ``sum(poss) / 5 / teams`` over ``war``'s rows (so pass whole league-seasons;
    the two agree unless a team has no player whose main team it was).
    Their ``war`` is ranked within the season (ties averaged) and the percentile
    ``100 * (n + 1 - rank) / (n + 1)`` picks the tier. Under the floor, with a null or non-finite ``war``,
    or in a season whose cohort is under :data:`MIN_COHORT_PLAYERS`, a player has no tier (null). With a
    ``conference_id`` column (college), ``war_tier_conf`` is the same ladder ranked within each
    ``(season, conference_id)`` over the same floor (null with no conference or a conference cohort under
    :data:`MIN_COHORT_PLAYERS`).

    Args:
        war: :func:`player_war` rows: ``season``, ``team_id``, ``poss`` and ``war``.

    Returns:
        ``war`` with ``war_tier`` (and with a ``conference_id``, ``war_tier_conf``; Utf8) added or replaced;
        nothing else changes.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``war`` lacks ``season``, ``team_id``, ``poss`` or
            ``war``.

    See Also:
        :func:`roster_tier_shape`: the league's median roster on this ladder.

    Example:
        Quick start::

            from sportsdataverse._common.player_war import war_tiers
            from sportsdataverse.nba.nba_player_war import nba_player_war
            war_tiers(nba_player_war(2026)).group_by("war_tier").len()
    """
    lg_team_poss = (
        pl.col("_lg_poss")
        if "_lg_poss" in war.columns
        else pl.col("poss").sum().over("season") / 5 / pl.col("team_id").n_unique().over("season")
    )
    cohort = war.with_columns(
        pl.when(pl.col("poss") >= TIER_MIN_POSS_SHARE * lg_team_poss).then(pl.col("war")).alias("_tier_war")
    )
    ranked = attach_ranks(cohort, ["_tier_war"], min_cohort=MIN_COHORT_PLAYERS, by=("season",))
    out = ranked.with_columns(_war_tier(pl.col("_tier_war_pct"))).drop("_tier_war_rank", "_tier_war_pct")
    if "conference_id" in war.columns:
        out = attach_ranks(out, ["_tier_war"], min_cohort=MIN_COHORT_PLAYERS, by=("season", "conference_id"))
        out = out.with_columns(_war_tier(pl.col("_tier_war_pct")).alias("war_tier_conf")).drop(
            "_tier_war_rank", "_tier_war_pct"
        )
    return out.drop("_tier_war")


def roster_tiers(war: pl.DataFrame) -> pl.DataFrame:
    """Each player on every team he played for (``team_ids``), with his full-season tiers.

    Args:
        war: :func:`player_war` rows: ``season``, ``player_id``, ``player_name``, ``team_id``, ``team_ids``,
            ``poss``, ``war``, ``war_tier`` and, in college, ``conference_id`` / ``war_tier_conf``.

    Returns:
        One row per ``(season, team_id, player_id)``: ``player_name``, ``main_team`` (Boolean: the team he
        played most possessions for), ``poss`` and ``war`` (his season's, not the team's share),
        ``war_tier`` and in college ``conference_id`` (that team's conference, from the players whose main
        team it is) and ``war_tier_conf``: his ladder step within that team's conference. On a team of
        another conference than his main team's, his WAR is placed among that conference's tier cohort (its
        tiered players, by main team, with him added); with no conference for the team it is null. Sorted
        by season, team and ``war`` (most first).

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``war`` lacks one of those columns.

    See Also:
        :func:`roster_tier_shape`: the league's median roster from these rows.
    """
    conf = "conference_id" in war.columns
    tiers = ["war_tier", *(["war_tier_conf"] if conf else [])]
    rows = (
        war.select(
            "season",
            "player_id",
            "player_name",
            pl.col("team_id").alias("_main"),
            pl.col("team_ids").str.split(",").alias("team_id"),
            "poss",
            "war",
            *tiers,
            *([pl.col("conference_id").alias("_main_conf")] if conf else []),
        )
        .explode("team_id")
        .drop_nulls("team_id")
        .with_columns(pl.col("team_id").cast(war.schema["team_id"]))
        .with_columns((pl.col("team_id") == pl.col("_main")).alias("main_team"))
        .drop("_main")
    )
    if conf:
        team_conf = war.select("season", "team_id", "conference_id").drop_nulls().unique(["season", "team_id"])
        rows = rows.join(team_conf, on=["season", "team_id"], how="left", validate="m:1")
        rows = _conference_tier_elsewhere(rows, war)
    cols = ["season", "team_id", "player_id", "player_name", "main_team", "poss", "war", "war_tier"]
    cols += ["conference_id", "war_tier_conf"] if conf else []
    return rows.select(cols).sort(["season", "team_id", "war"], descending=[False, False, True], nulls_last=True)


def _conference_tier_elsewhere(rows: pl.DataFrame, war: pl.DataFrame) -> pl.DataFrame:
    """``war_tier_conf`` of a :func:`roster_tiers` row on a team outside his main team's conference.

    His full-season WAR is ranked among that conference's tier cohort (its tiered players, by their main
    team) with him added, ties averaged, and the percentile ``100 * (n + 1 - rank) / (n + 1)`` over those
    ``n`` players picks the :data:`WAR_TIERS` step. Null when he has no ``war_tier`` (under the floor), the
    team has no conference, or the conference's cohort without him is under :data:`MIN_COHORT_PLAYERS`.
    Rows on a team of his own conference keep the :func:`war_tiers` value.
    """
    keys = ["season", "conference_id"]
    away = (pl.col("conference_id") != pl.col("_main_conf")).fill_null(True) & (pl.col("main_team") == False)
    moved = rows.filter(away & pl.col("conference_id").is_not_null() & pl.col("war_tier").is_not_null())
    cohort = war.filter(pl.col("war_tier").is_not_null() & pl.col("conference_id").is_not_null()).select(
        *keys, pl.col("war").alias("_cw")
    )
    placed = (
        moved.select(*keys, "team_id", "player_id", "war")
        .join(cohort, on=keys, how="left")
        .group_by(*keys, "team_id", "player_id")
        .agg(
            pl.col("_cw").count().alias("_n"),
            (pl.col("_cw") > pl.col("war")).sum().alias("_gt"),
            (pl.col("_cw") == pl.col("war")).sum().alias("_eq"),
        )
        .with_columns((pl.col("_n") + 1).alias("_n1"), (pl.col("_gt") + 1 + pl.col("_eq") / 2).alias("_rank"))
        .with_columns(
            pl.when(pl.col("_n") >= MIN_COHORT_PLAYERS)
            .then(100 * (pl.col("_n1") + 1 - pl.col("_rank")) / (pl.col("_n1") + 1))
            .alias("_pct")
        )
        .select(*keys, "team_id", "player_id", _war_tier(pl.col("_pct")).alias("_conf_tier"))
    )
    return (
        rows.join(placed, on=[*keys, "team_id", "player_id"], how="left", validate="1:1")
        .with_columns(
            pl.when(away).then(pl.col("_conf_tier")).otherwise(pl.col("war_tier_conf")).alias("war_tier_conf")
        )
        .drop("_conf_tier", "_main_conf")
    )


def _tier_frame() -> pl.DataFrame:
    """The tiers in order with their percentile bands (``pct_ceiling`` null above the top floor)."""
    floors = list(WAR_TIERS.values())
    return pl.DataFrame(
        {
            "war_tier": list(WAR_TIERS),
            "tier_order": list(range(1, len(WAR_TIERS) + 1)),
            "pct_floor": floors,
            "pct_ceiling": [None, *floors[:-1]],
        },
        schema={"war_tier": pl.Utf8, "tier_order": pl.Int64, "pct_floor": pl.Float64, "pct_ceiling": pl.Float64},
    )


def _team_tier_counts(rows: pl.DataFrame, tier: str = "war_tier", by: tuple[str, ...] = ()) -> pl.DataFrame:
    """Players per ``(season, *by, team_id, tier)`` of :func:`roster_tiers` rows, zero rows included.

    The teams are those with a player tiered in ``tier`` that season; a traded player counts on each.
    """
    keys = ["season", *by, "team_id"]
    tiered = rows.filter(pl.col(tier).is_not_null() & pl.all_horizontal(pl.col(k).is_not_null() for k in keys))
    counts = tiered.group_by(*keys, pl.col(tier).alias("war_tier")).agg(pl.len().cast(pl.Int64).alias("players"))
    return (
        tiered.select(keys)
        .unique()
        .join(_tier_frame(), how="cross")
        .join(counts, on=[*keys, "war_tier"], how="left")
        .with_columns(pl.col("players").fill_null(0))
    )


#: :func:`roster_tier_shape`'s columns.
TIER_SHAPE_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "scope": pl.Utf8(),
    "conference_id": pl.Utf8(),
    "war_tier": pl.Utf8(),
    "tier_order": pl.Int64(),
    "pct_floor": pl.Float64(),
    "pct_ceiling": pl.Float64(),
    "players_median": pl.Float64(),
    "players_mean": pl.Float64(),
    "teams": pl.Int64(),
}


def roster_tier_shape(war: pl.DataFrame) -> pl.DataFrame:
    """The median roster on the :data:`WAR_TIERS` ladder: one row per season, scope and tier.

    From :func:`roster_tiers`: each team's count of players in each tier (zero when it has none; a traded
    player counts on every team he played for, with his full-season tier), then the median over the teams.
    ``scope`` ``league`` is every team of the league-season by ``war_tier`` (``conference_id`` null);
    ``scope`` ``conference`` (college) is each conference's teams by ``war_tier_conf``. The medians of the
    five tiers need not add up to the median roster, and because a traded player is on two rosters the
    means add up to the average roster of players used, more than the league's players over its teams.

    Args:
        war: :func:`player_war` rows (any seasons of one league), with ``war_tier``.

    Returns:
        :data:`TIER_SHAPE_SCHEMA` rows: ``season``, ``scope``, ``conference_id`` (Utf8), ``war_tier``,
        ``tier_order`` (1 = ``Star``), ``pct_floor`` / ``pct_ceiling`` (the tier's band of the tier-cohort
        WAR percentile, floor inclusive, ceiling exclusive, null ceiling for ``Star``; :data:`WAR_TIERS`),
        ``players_median`` (the median over teams of the players in that tier), ``players_mean`` and
        ``teams`` (the teams with a tiered player in that scope). Sorted by season, scope, conference and
        tier. An input with no tiered player returns that schema with zero rows.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``war`` lacks a :func:`roster_tiers` column.

    See Also:
        :func:`war_tiers`: the tier cohort and the cut points behind ``war_tier``.
        :func:`roster_tiers`: the team rows the medians are over.

    Example:
        Quick start::

            from sportsdataverse._common.player_war import roster_tier_shape
            from sportsdataverse.nba.nba_player_war import nba_player_war
            roster_tier_shape(nba_player_war(2026))
    """
    rows = roster_tiers(war)
    scopes: list[tuple[str, str, tuple[str, ...]]] = [("league", "war_tier", ())]
    if "war_tier_conf" in rows.columns:
        scopes.append(("conference", "war_tier_conf", ("conference_id",)))
    out = []
    for scope, tier, by in scopes:
        shape = (
            _team_tier_counts(rows, tier, by)
            .group_by("season", *by, "war_tier", "tier_order", "pct_floor", "pct_ceiling")
            .agg(
                pl.col("players").median().cast(pl.Float64).alias("players_median"),
                pl.col("players").mean().cast(pl.Float64).alias("players_mean"),
                pl.len().cast(pl.Int64).alias("teams"),
            )
            .with_columns(pl.lit(scope).alias("scope"))
        )
        if not by:
            shape = shape.with_columns(pl.lit(None, pl.Utf8).alias("conference_id"))
        out.append(shape.select(list(TIER_SHAPE_SCHEMA)).cast(TIER_SHAPE_SCHEMA))
    return pl.concat(out).sort(
        "season", "scope", "conference_id", "tier_order", descending=[False, True, False, False], nulls_last=False
    )


def player_war_league_averages(war: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """The league-average rows of a ``player_war`` table: one per season and ranked column.

    The football F1 rows of :func:`~sportsdataverse._common.league_context.league_averages` over the
    players each column is ranked among: every player of the season for ``vorp`` / ``war``, the players
    over the games gate for the per-game columns. ``n`` is that cohort, the number a ``_rank`` is out
    of; it is reported even where the ranks are null (under ten players, or all equal).

    Args:
        war: :func:`player_war` rows (any seasons of one league).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: the ``level`` label and the games gates.

    Returns:
        :data:`~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA` rows, ``entity``
        ``"player"``, ``category`` ``"player_war"``, ``qualifier_min`` null; an empty input returns
        that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse._common.player_war import player_war_league_averages
            from sportsdataverse.nba.nba_player_war import nba_player_war
            player_war_league_averages(nba_player_war(2026), league="nba")
    """
    return league_averages(
        war,
        ["vorp", "war", *PER_GAME],
        level=league,
        entity="player",
        category="player_war",
        min_n=player_box_gates(league, PER_GAME),
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
