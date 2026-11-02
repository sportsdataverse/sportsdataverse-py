"""College football standings, tiebreakers, and CFP seeding.

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe):
https://nflseedr.com. Architecture mirrors nflseedR's
``standings_init -> ranks -> seeds`` pipeline (``R/standings.R``,
``R/standings_init.R``, ``R/conference_tiebreaker.R``) with the CFB
adaptations from the shared seedr-port spec.

CFB simplifications (documented deliberately — the R ``cfbseedR`` port uses
the SAME semantics so outputs cross-validate):

* **Conferences instead of divisions.** Standings group by ``conference``;
  there is no division layer and no draft order.
* **Independents** (``conference`` null or ``"FBS Independents"``) are
  included in overall standings but excluded from conference ranks
  (``conf_rank`` is null) and can never be conference champions.
* **Generic tiebreaker cascade.** Real CFB tiebreakers are per-conference;
  this engine applies one documented cascade to every conference:
  conference win pct (primary sort) -> head-to-head among tied ->
  record vs common conference opponents (min 1) -> SOV -> SOS ->
  conference point differential -> coin flip. Steps are gated
  by ``tiebreaker_depth`` exactly like nflseedR
  (``RANDOM=0 < PRE-SOV=1 < SOS=2 < POINTS=3``).
* **Record semantics.** Overall W/L/T counts ALL played games. The
  conference record counts only ``game_type == "REG"`` games between two
  teams of the same conference. ``CONF_CHAMP`` games count toward the
  overall record and decide the conference champion, but NOT the
  conference record/rank.
* **SOV/SOS are conference-scoped** (cross-validation ruling — UNLIKE
  nflseedR's overall-REG, games-weighted convention). Both use only
  conference REG games and the opponents' conference win pct:
  ``sos`` = mean of conference opponents' conference win pct across all
  conference games played (per game — a twice-played opponent counts
  twice); ``sov`` = mean of defeated conference opponents' conference win
  pct (per conference victory — a twice-beaten opponent contributes twice;
  documented choice, unobservable on the toy fixture). Independents and
  teams without conference games/wins get 0.0. The ENTIRE tiebreaker
  cascade (head-to-head, common opponents, SOV, SOS, point differential)
  is likewise scoped to conference REG games.
* **Official per-conference tiebreakers (registry).** ``CONFERENCE_TIEBREAKERS``
  registers the official procedures for the SEC, Big Ten, Big 12, ACC, MAC,
  American, Conference USA, Mountain West and Sun Belt (cfbseedR 0.2.0
  parity) as season-scoped epochs of plain rung-dict lists; any conference not
  in the registry (e.g. the re-formed Pac-12, which has not published one)
  uses the generic cascade above, completely unchanged. Registry cascades
  always run in full (``tiebreaker_depth`` gates only the generic fallback).
  An epoch applies from its ``from`` season: games keyed by ``season`` (or an
  explicit ``season=``) resolve per season, plain ``sim`` ids use the current
  rules — so the ACC's 2026 policy (head-to-head, SportSource Team Success
  Ranking, draw, plus the alternate-game-count candidate pool) applies from
  2026 while earlier ACC seasons keep the 2024 cascade. Optional ``teams``
  columns ``conf_division`` (division format: the two division champions take
  ``conf_rank`` 1-2, as in the 2026 Sun Belt) and ``postseason_eligible`` (an
  explicit ``False`` team cannot hold a championship-game berth; its games
  still count) feed the same ranking.
  Adopted ambiguity resolutions (per the shared design brief): "combined win
  percentage of conference opponents" is POOLED (sum of opponents' conference
  wins / sum of their conference games, opponents counted once per game
  played) — distinct from the per-game-mean ``sos`` column; the Big 12
  grouped-ties descent rule (compare records vs a tied group of common
  opponents collectively, prior to that group's own tiebreak) applies to ALL
  registry descent rungs; multi-team combined head-to-head applies only when
  every tied pair played (otherwise only the defeated-all elimination
  applies, seeding the team that beat every other tied team it played;
  ``ponytail:`` the symmetric lost-to-all elimination is NOT separately
  modeled — its poor within-group record already surfaces at
  ``record_vs_common``/``opp_conf_win_pct`` in practice, a documented
  first-cut simplification, see ``_apply_h2h``); after each team is seeded
  the procedure RESTARTS from rung 1 with the remaining set, and a
  reduction to two switches to the 2-team walk. The SEC capped scoring
  margin is per-game
  (42 scored / 48 allowed), summed across conference games. Under registry
  conferences ``conf_rank`` 1-2 are the championship-game participants.
  Rungs whose optional inputs are absent (per-game points, an FBS/FCS
  ``division`` flag on ``teams``, analytics ratings, CFP rankings, APR, a
  ``conf_division`` column) are skipped deterministically; each skip is
  recorded in ``tiebreak_notes`` (see :func:`cfb_standings`). Big 12
  NCAA-exempt games are not modeled.
* **CFP selection rule evolves by season.** :func:`cfb_playoff_seeds` holds
  both 12-team straight-seeding rules in ONE function: the 2026 rule (P4
  champions, best Group-of-6 champion, Notre Dame if top 12, at-large) and
  the 2024-2025 rule (5 highest-ranked champions, at-large).
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, cast

import numpy as np
import polars as pl

__all__ = ["cfb_standings", "cfb_playoff_seeds", "cfb_games_from_schedule", "CONFERENCE_TIEBREAKERS"]

TIEBREAKER_DEPTHS: Dict[str, int] = {"RANDOM": 0, "PRE-SOV": 1, "SOS": 2, "POINTS": 3}
INDEPENDENT_LABEL = "FBS Independents"
_VALID_GAME_TYPES = ("REG", "CONF_CHAMP", "POST")

# rec map: sim -> team -> opp -> [outcome_points, n_games] (conference REG games only)
_RecMap = Dict[Any, Dict[str, Dict[str, List[float]]]]
_RecSim = Dict[str, Dict[str, List[float]]]
# per-team cascade metrics: metric name -> value (None = optional input unavailable)
_Metrics = Dict[str, Dict[str, Optional[float]]]
_Rung = Dict[str, Any]

FrameLike = Union[pl.DataFrame, Any]

# ponytail: caps are module constants (only the SEC / Big 12 use these rungs);
# the registry rung params carry them for cross-language fidelity — make the
# precompute read the rung params if a conference ever registers different caps.
_SEC_OFF_CAP = 42.0
_SEC_DEF_CAP = 48.0
_B12_FCS_CAP = 1

# The generic nflseedR-style cascade expressed as a rung list; ``min_depth`` is
# the ``tiebreaker_depth`` gate. There is ONE execution path — the registry
# cascades below are just other rung lists (no ``min_depth``: official
# procedures always run in full).
_GENERIC_CASCADE: List[_Rung] = [
    {"kind": "h2h", "min_depth": 1, "generic": True},
    {"kind": "record_vs_common", "min_depth": 1},
    {"kind": "sov", "min_depth": 2},
    {"kind": "sos", "min_depth": 2},
    {"kind": "conf_pd", "min_depth": 3},
    {"kind": "coin_toss", "min_depth": 0},
]

# Big Ten / ACC / MAC share one official template (h2h, common opponents,
# order-of-finish descent, pooled opponents' conference win pct, SportSource
# analytics rating, draw).
_P5_TEMPLATE: List[_Rung] = [
    {"kind": "h2h"},
    {"kind": "record_vs_common"},
    {"kind": "record_vs_common_desc", "mode": "order_of_finish", "grouped_ties": True},
    {"kind": "opp_conf_win_pct"},
    {"kind": "analytics_rating"},
    {"kind": "coin_toss"},
]

# ACC policy effective 2026: head-to-head, SportSource Team Success Ranking,
# draw. Its epoch's ``pool="wins_or_losses"`` admits teams that played an
# alternate number of conference games into the leaders' tier when they match
# the leaders' win OR loss count.
_ACC_2026: List[_Rung] = [{"kind": "h2h"}, {"kind": "analytics_rating"}, {"kind": "coin_toss"}]

# MAC published procedure: head-to-head / defeated-all, record vs common
# conference opponents, SportSource Analytics Team Rating Score.
_MAC_OFFICIAL: List[_Rung] = [
    {"kind": "h2h"},
    {"kind": "record_vs_common"},
    {"kind": "analytics_rating"},
    {"kind": "coin_toss"},
]

# American: head-to-head, the CFP-ranked final-week clause, the conference's
# metric composite (supply it as ``tiebreaker_data["analytics_ratings"]``),
# record vs common opponents.
_AMERICAN_OFFICIAL: List[_Rung] = [
    {"kind": "h2h"},
    {"kind": "cfp_ranked_final_week"},
    {"kind": "analytics_rating"},
    {"kind": "record_vs_common"},
    {"kind": "coin_toss"},
]

# Conference USA: head-to-head, CFP-ranked final-week clause, metric composite,
# multi-year Academic Progress Rate (``tiebreaker_data["apr"]``), draw.
_CUSA_OFFICIAL: List[_Rung] = [
    {"kind": "h2h"},
    {"kind": "cfp_ranked_final_week"},
    {"kind": "analytics_rating"},
    {"kind": "apr"},
    {"kind": "coin_toss"},
]

# Mountain West: berth ties go straight to the metric composite; head-to-head
# is the fallback when no composite is supplied.
_MW_OFFICIAL: List[_Rung] = [{"kind": "analytics_rating"}, {"kind": "h2h"}, {"kind": "coin_toss"}]

# Sun Belt (division format): head-to-head, divisional win pct, descent with
# grouped ties, record vs common opponents, CFP-ranked final-week clause,
# computer-ranking composite.
_SUNBELT_OFFICIAL: List[_Rung] = [
    {"kind": "h2h"},
    {"kind": "div_pct"},
    {"kind": "record_vs_common_desc", "mode": "order_of_finish", "grouped_ties": True},
    {"kind": "record_vs_common"},
    {"kind": "cfp_ranked_final_week"},
    {"kind": "analytics_rating"},
    {"kind": "coin_toss"},
]


def _epoch(rungs: List[_Rung], start: int = 0, pool: Optional[str] = None) -> Dict[str, Any]:
    return {"from": start, "rungs": rungs, "pool": pool}


#: Official conference tiebreaker procedures, mirroring cfbseedR 0.2.0's
#: ``CONFERENCE_TIEBREAKERS``: conference -> list of season-scoped epochs
#: ``{"from": <first season>, "rungs": [<rung dict>, ...], "pool": <str|None>}``.
#: :func:`cfb_standings` applies the epoch with the largest ``from`` not
#: exceeding the season being ranked (unknown season = current rules). The
#: cross-language parity fixtures under ``tests/fixtures/seedr/`` pin the
#: semantics. Conferences not registered here use the generic cascade.
CONFERENCE_TIEBREAKERS: Dict[str, List[Dict[str, Any]]] = {
    "SEC": [
        _epoch(
            [
                {"kind": "h2h"},
                {"kind": "record_vs_common"},
                {"kind": "record_vs_common_desc", "mode": "order_of_finish", "grouped_ties": True},
                {"kind": "opp_conf_win_pct"},
                {"kind": "capped_scoring_margin", "off_cap": 42, "def_cap": 48},
                {"kind": "coin_toss"},
            ]
        )
    ],
    "Big Ten": [_epoch(_P5_TEMPLATE)],
    "ACC": [_epoch(_P5_TEMPLATE), _epoch(_ACC_2026, 2026, pool="wins_or_losses")],
    "MAC": [_epoch(_MAC_OFFICIAL)],
    "Mid-American": [_epoch(_MAC_OFFICIAL)],  # cfbfastR schedule naming for the MAC
    "Big 12": [
        _epoch(
            [
                {"kind": "h2h"},
                {"kind": "record_vs_common"},
                {"kind": "record_vs_common_desc", "mode": "next_highest", "grouped_ties": True},
                {"kind": "opp_conf_win_pct"},
                {"kind": "total_wins", "fcs_cap": 1},
                {"kind": "analytics_rating"},
                {"kind": "coin_toss"},
            ]
        )
    ],
    "American Athletic": [_epoch(_AMERICAN_OFFICIAL)],
    "American": [_epoch(_AMERICAN_OFFICIAL)],
    "Conference USA": [_epoch(_CUSA_OFFICIAL)],
    "Mountain West": [_epoch(_MW_OFFICIAL)],
    "Sun Belt": [_epoch(_SUNBELT_OFFICIAL)],
}


def _conf_rules(conf_name: str, season: Optional[int]) -> Tuple[List[_Rung], Optional[str]]:
    """(rungs, pool) for one conference-season; unregistered -> the generic cascade."""
    epochs = CONFERENCE_TIEBREAKERS.get(conf_name)
    if not epochs:
        return _GENERIC_CASCADE, None
    eligible = [e for e in epochs if season is None or e["from"] <= season] or epochs[:1]
    e = max(eligible, key=lambda x: x["from"])
    return e["rungs"], e.get("pool")


def _to_polars(df: FrameLike) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    return pl.from_pandas(df)


def _is_independent(col: str = "conference") -> pl.Expr:
    return pl.col(col).is_null() | (pl.col(col) == INDEPENDENT_LABEL)


def _validate_teams(teams: FrameLike) -> pl.DataFrame:
    t = _to_polars(teams)
    for c in ("team", "conference"):
        if c not in t.columns:
            raise ValueError(f"`teams` must contain a `{c}` column")
    cols = [pl.col("team").cast(pl.Utf8), pl.col("conference").cast(pl.Utf8)]
    if "division" in t.columns:  # optional FBS/FCS flag (Big 12 total_wins FCS cap)
        cols.append(pl.col("division").cast(pl.Utf8))
    if "conf_division" in t.columns:  # optional in-conference division (Sun Belt format, div_pct rung)
        cols.append(pl.col("conf_division").cast(pl.Utf8))
    if "postseason_eligible" in t.columns:  # optional; only an explicit False marks a team out
        cols.append(pl.col("postseason_eligible").cast(pl.Boolean))
    t = t.select(cols)
    if t["team"].n_unique() != t.height:
        raise ValueError("`teams` contains duplicate team rows")
    return t


def _validate_games(games: FrameLike) -> pl.DataFrame:
    g = _to_polars(games)
    if "sim" not in g.columns:
        if "season" in g.columns:
            g = g.rename({"season": "sim"})
        else:
            raise ValueError("`games` must contain a `sim` or `season` column")
    required = ("week", "game_type", "home_team", "away_team", "result")
    missing = [c for c in required if c not in g.columns]
    if missing:
        raise ValueError(f"`games` is missing required column(s): {missing}")
    if "neutral" not in g.columns:
        g = g.with_columns(pl.lit(0, dtype=pl.Int64).alias("neutral"))
    bad = g.filter(~pl.col("game_type").is_in(list(_VALID_GAME_TYPES)))
    if bad.height > 0:
        raise ValueError(
            f"`games.game_type` must be one of {_VALID_GAME_TYPES}; got {bad['game_type'].unique().to_list()}"
        )
    cols = [
        pl.col("sim").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        pl.col("game_type").cast(pl.Utf8),
        pl.col("home_team").cast(pl.Utf8),
        pl.col("away_team").cast(pl.Utf8),
        pl.col("result").cast(pl.Float64),
        pl.col("neutral").cast(pl.Int64),
    ]
    if "home_points" in g.columns and "away_points" in g.columns:
        # optional per-game points (SEC capped-scoring-margin tiebreaker input)
        cols += [pl.col("home_points").cast(pl.Float64), pl.col("away_points").cast(pl.Float64)]
    return g.select(cols)


def _double_games(g: pl.DataFrame) -> pl.DataFrame:
    """Long form: one row per (game, perspective team), played games only."""
    has_pts = "home_points" in g.columns and "away_points" in g.columns
    _null_f64 = pl.lit(None, dtype=pl.Float64)
    home = g.select(
        "sim",
        "week",
        "game_type",
        pl.col("home_team").alias("team"),
        pl.col("away_team").alias("opp"),
        pl.col("result"),
        (pl.col("home_points") if has_pts else _null_f64).alias("pf"),
        (pl.col("away_points") if has_pts else _null_f64).alias("pa"),
    )
    away = g.select(
        "sim",
        "week",
        "game_type",
        pl.col("away_team").alias("team"),
        pl.col("home_team").alias("opp"),
        (-pl.col("result")).alias("result"),
        (pl.col("away_points") if has_pts else _null_f64).alias("pf"),
        (pl.col("home_points") if has_pts else _null_f64).alias("pa"),
    )
    dg = pl.concat([home, away]).filter(pl.col("result").is_not_null())
    return dg.with_columns(
        pl.when(pl.col("result") > 0).then(1.0).when(pl.col("result") < 0).then(0.0).otherwise(0.5).alias("outcome")
    )


def _standings_base(g: pl.DataFrame, t: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Raw standings (records, pct, SOV/SOS) per (sim, team) + doubled REG frame."""
    dg = _double_games(g)
    sims = g.select("sim").unique()
    base = sims.join(t, how="cross")

    overall = dg.group_by("sim", "team").agg(
        pl.len().cast(pl.Int64).alias("games"),
        (pl.col("outcome") == 1.0).sum().cast(pl.Int64).alias("wins"),
        (pl.col("outcome") == 0.0).sum().cast(pl.Int64).alias("losses"),
        (pl.col("outcome") == 0.5).sum().cast(pl.Int64).alias("ties"),
        pl.col("result").sum().cast(pl.Float64).alias("pd"),
    )

    dg_reg = dg.filter(pl.col("game_type") == "REG")
    dgc = (
        dg_reg.join(t.rename({"conference": "team_conf"}), on="team", how="left")
        .join(t.rename({"team": "opp", "conference": "opp_conf"}), on="opp", how="left")
        .filter(
            (pl.col("team_conf") == pl.col("opp_conf")) & (_is_independent("team_conf") == False)  # noqa: E712
        )
    )
    conf = dgc.group_by("sim", "team").agg(
        pl.len().cast(pl.Int64).alias("conf_games"),
        (pl.col("outcome") == 1.0).sum().cast(pl.Int64).alias("conf_wins"),
        (pl.col("outcome") == 0.0).sum().cast(pl.Int64).alias("conf_losses"),
        (pl.col("outcome") == 0.5).sum().cast(pl.Int64).alias("conf_ties"),
        pl.col("result").sum().cast(pl.Float64).alias("conf_pd"),
    )

    # Conference-scoped SOV/SOS (cross-validation ruling): mean of conference
    # opponents' conference win pct — per conference game for SOS, per
    # conference victory for SOV. Every opp in `dgc` has a `conf` row (it
    # played at least this conference game), so the join never misses.
    conf_pct_opp = conf.select(
        "sim",
        pl.col("team").alias("opp"),
        ((pl.col("conf_wins") + 0.5 * pl.col("conf_ties")) / pl.col("conf_games"))
        .cast(pl.Float64)
        .alias("_opp_conf_pct"),
        (pl.col("conf_wins") + 0.5 * pl.col("conf_ties")).cast(pl.Float64).alias("_opp_conf_w"),
        pl.col("conf_games").cast(pl.Float64).alias("_opp_conf_g"),
    )
    sovsos = (
        dgc.join(conf_pct_opp, on=["sim", "opp"], how="left")
        .with_columns(won=(pl.col("outcome") == 1.0).cast(pl.Float64))
        .group_by("sim", "team")
        .agg(
            pl.col("_opp_conf_pct").mean().cast(pl.Float64).alias("sos"),
            (pl.col("_opp_conf_pct") * pl.col("won")).sum().alias("sov_num"),
            pl.col("won").sum().alias("sov_den"),
            # POOLED opponents' conference win pct (registry `opp_conf_win_pct`
            # rung — ambiguity resolution 1; distinct from the per-game-mean sos)
            pl.col("_opp_conf_w").sum().alias("_pool_num"),
            pl.col("_opp_conf_g").sum().alias("_pool_den"),
        )
        .with_columns(
            pl.when(pl.col("sov_den") > 0)
            .then(pl.col("sov_num") / pl.col("sov_den"))
            .otherwise(0.0)
            .cast(pl.Float64)
            .alias("sov"),
            pl.when(pl.col("_pool_den") > 0)
            .then(pl.col("_pool_num") / pl.col("_pool_den"))
            .otherwise(0.0)
            .cast(pl.Float64)
            .alias("_opp_wp_pooled"),
        )
        .select("sim", "team", "sov", "sos", "_opp_wp_pooled")
    )

    # SEC capped relative scoring margin over conference games (per game:
    # points scored capped at 42, points allowed capped at 48); null when any
    # conference game lacks per-game points (the rung is then skipped + noted).
    has_pts = "home_points" in g.columns and "away_points" in g.columns
    if has_pts:
        capped_m = (
            dgc.group_by("sim", "team")
            .agg(
                (pl.col("pf").is_null() | pl.col("pa").is_null()).sum().alias("_pts_null"),
                (
                    pl.min_horizontal(pl.col("pf"), pl.lit(_SEC_OFF_CAP))
                    - pl.min_horizontal(pl.col("pa"), pl.lit(_SEC_DEF_CAP))
                )
                .sum()
                .alias("_cm_sum"),
            )
            .with_columns(
                pl.when(pl.col("_pts_null") > 0)
                .then(None)
                .otherwise(pl.col("_cm_sum"))
                .cast(pl.Float64)
                .alias("_capped_margin")
            )
            .select("sim", "team", "_capped_margin")
        )
    else:
        capped_m = None

    # Big 12 total wins with the FCS cap (max `_B12_FCS_CAP` wins vs an
    # FCS-or-lower opponent counted). An opponent counts as FCS-or-lower when
    # it is absent from `teams` entirely, or carries a non-FBS `division`.
    # Null column when `teams` has no `division` flag (fallback: raw wins + note).
    if "division" in t.columns:
        opp_info = t.select(
            pl.col("team").alias("opp"),
            pl.col("division").alias("_opp_div"),
            pl.lit(True).alias("_opp_known"),
        )
        is_fcs = (pl.col("_opp_known").fill_null(False) == False) | (  # noqa: E712
            pl.col("_opp_div").is_not_null() & (pl.col("_opp_div").str.to_lowercase() != "fbs")
        )
        capped_w = (
            dg.join(opp_info, on="opp", how="left")
            .group_by("sim", "team")
            .agg(
                ((pl.col("outcome") == 1.0) & is_fcs).sum().cast(pl.Float64).alias("_fcs_w"),
                (pl.col("outcome") == 1.0).sum().cast(pl.Float64).alias("_all_w"),
            )
            .with_columns(
                (pl.col("_all_w") - pl.col("_fcs_w") + pl.min_horizontal(pl.col("_fcs_w"), pl.lit(float(_B12_FCS_CAP))))
                .cast(pl.Float64)
                .alias("_capped_wins")
            )
            .select("sim", "team", "_capped_wins")
        )
    else:
        capped_w = None

    # cfp_ranked_final_week rung input: did the team win every conference REG
    # game of its final conference-game week?
    final_win = (
        dgc.filter(pl.col("week") == pl.col("week").max().over("sim", "team"))
        .group_by("sim", "team")
        .agg((pl.col("outcome") == 1.0).all().alias("_final_conf_win"))
    )

    # div_pct rung input: win pct over conference REG games vs same-division
    # opponents (0.0 with no such game); null column without `conf_division`.
    if "conf_division" in t.columns:
        div_pct = (
            dgc.join(t.select("team", pl.col("conf_division").alias("_td")), on="team", how="left")
            .join(t.select(pl.col("team").alias("opp"), pl.col("conf_division").alias("_od")), on="opp", how="left")
            .filter(pl.col("_td").is_not_null() & (pl.col("_td") == pl.col("_od")))
            .group_by("sim", "team")
            .agg(pl.col("outcome").mean().cast(pl.Float64).alias("_div_pct"))
        )
    else:
        div_pct = None

    st = (
        base.join(overall, on=["sim", "team"], how="left")
        .join(conf, on=["sim", "team"], how="left")
        .join(sovsos, on=["sim", "team"], how="left")
        .with_columns(
            pl.col("games", "wins", "losses", "ties").fill_null(0),
            pl.col("conf_games", "conf_wins", "conf_losses", "conf_ties").fill_null(0),
            # ponytail: pd/conf_pd/sov/sos filled 0.0 for zero-game teams (nflseedR uses NA
            # for conf_pd) — the cascade only compares same-conference teams, all of which
            # have conference games in practice.
            pl.col("pd", "conf_pd", "sov", "sos", "_opp_wp_pooled").fill_null(0.0),
        )
    )
    if capped_m is not None:
        st = st.join(capped_m, on=["sim", "team"], how="left")
    else:
        st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias("_capped_margin"))
    if capped_w is not None:
        st = st.join(capped_w, on=["sim", "team"], how="left").with_columns(
            # division flag present: teams with no played games simply have 0 capped wins
            pl.col("_capped_wins").fill_null(0.0)
        )
    else:
        # no `division` flag on `teams`: degrade to uncapped wins (Part 2 of the design
        # brief: "without it count all wins + note") rather than skip the rung outright.
        st = st.with_columns(pl.col("wins").cast(pl.Float64).alias("_capped_wins"))
    st = st.join(final_win, on=["sim", "team"], how="left").with_columns(pl.col("_final_conf_win").fill_null(False))
    if div_pct is not None:
        st = st.join(div_pct, on=["sim", "team"], how="left").with_columns(pl.col("_div_pct").fill_null(0.0))
    else:
        st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias("_div_pct"))
    st = st.with_columns(
        pl.when(pl.col("games") > 0)
        .then((pl.col("wins") + 0.5 * pl.col("ties")) / pl.col("games"))
        .otherwise(0.0)
        .cast(pl.Float64)
        .alias("win_pct"),
        pl.when(pl.col("conf_games") > 0)
        .then((pl.col("conf_wins") + 0.5 * pl.col("conf_ties")) / pl.col("conf_games"))
        .otherwise(0.0)
        .cast(pl.Float64)
        .alias("conf_pct"),
    )
    return st, dgc


def _build_rec_map(dg_conf: pl.DataFrame) -> _RecMap:
    """Per-sim head-to-head map from doubled conference REG games.

    ponytail: python dict build — O(rows) and fine at test/sim scale (100 sims);
    vectorize per-step if 10k-sim standings ever become a hot path.
    """
    rec: _RecMap = {}
    for sim, team, opp, outcome in dg_conf.select("sim", "team", "opp", "outcome").iter_rows():
        d = rec.setdefault(sim, {}).setdefault(team, {}).setdefault(opp, [0.0, 0.0])
        d[0] += outcome
        d[1] += 1.0
    return rec


def _keep_max(cands: List[str], key: Dict[str, float]) -> List[str]:
    m = max(key[c] for c in cands)
    return [c for c in cands if key[c] >= m - 1e-12]


def _pct_vs(rec_sim: Dict[str, Dict[str, List[float]]], team: str, opps: Sequence[str]) -> Tuple[float, float]:
    pts, n = 0.0, 0.0
    d = rec_sim.get(team, {})
    for o in opps:
        if o in d:
            pts += d[o][0]
            n += d[o][1]
    return pts, n


@dataclass
class _TieCtx:
    """Per-(sim, conference) tiebreak context threaded through rung dispatch.

    ``notes`` is the shared, mutable ``tiebreak_notes`` list for the whole
    :func:`cfb_standings` call (see the module docstring's "Official
    per-conference tiebreakers" section) — messages are deduplicated so a
    rung skipped across many tied groups/sims only logs once.
    """

    conf_name: str
    conf_pct_by_team: Dict[str, float]
    division_absent: bool
    notes: List[str]

    def note(self, msg: str) -> None:
        if msg not in self.notes:
            self.notes.append(msg)


def _apply_h2h(cands: List[str], rec_sim: Dict[str, Dict[str, List[float]]], generic: bool) -> List[str]:
    """Head-to-head rung: generic (existing, unchanged) vs registry multi-team semantics."""
    if generic or len(cands) == 2:
        # generic cascade (byte-identical to the pre-registry implementation) and the
        # registry's 2-team walk (same aggregate-vs-the-rest computation for n=2).
        h2h = {c: _pct_vs(rec_sim, c, [x for x in cands if x != c]) for c in cands}
        if all(n > 0 for _, n in h2h.values()):
            return _keep_max(cands, {c: p / n for c, (p, n) in h2h.items()})
        return cands
    # registry multi-team (3+ tied): combined head-to-head only applies when every
    # tied pair actually played each other; otherwise only the defeated-all /
    # lost-to-all elimination applies (ambiguity resolution 3 in the design brief).
    pairs_played = all(rec_sim.get(a, {}).get(b, [0.0, 0.0])[1] > 0 for a in cands for b in cands if a != b)
    if pairs_played:
        pct = {c: _pct_vs(rec_sim, c, [x for x in cands if x != c]) for c in cands}
        return _keep_max(cands, {c: (p / n if n > 0 else 0.0) for c, (p, n) in pct.items()})
    for c in cands:
        others = [o for o in cands if o != c]
        recs = [rec_sim.get(c, {}).get(o) for o in others]
        if all(r is not None and r[1] > 0 for r in recs):
            assert recs  # narrows Optional for mypy; guarded by the all() above
            if all(r[0] == r[1] for r in recs if r is not None):
                return [c]  # defeated every other tied team it played -> sole front-runner
            # ponytail: lost-to-all elimination (a team that lost every head-to-head it
            # played within the group) is not separately modeled as a back-of-group
            # placement — its poor within-group record already surfaces at
            # record_vs_common / opp_conf_win_pct in the rungs below, which is a
            # documented simplification for this first cut. Upgrade path: track an
            # explicit "confirmed-last" set here if a real fixture needs it.
    return cands


def _apply_record_vs_common(cands: List[str], rec_sim: Dict[str, Dict[str, List[float]]]) -> List[str]:
    """Record vs ALL common conference opponents of the tied set (min 1 shared opponent)."""
    if len(cands) <= 1:
        return cands
    opp_sets = [set(rec_sim.get(c, {})) for c in cands]
    commons = set.intersection(*opp_sets) - set(cands) if opp_sets else set()
    if not commons:
        return cands
    vs = {c: _pct_vs(rec_sim, c, sorted(commons)) for c in cands}
    return _keep_max(cands, {c: (p / n if n > 0 else 0.0) for c, (p, n) in vs.items()})


def _apply_record_vs_common_desc(
    cands: List[str],
    rec_sim: Dict[str, Dict[str, List[float]]],
    conf_pct_by_team: Dict[str, float],
) -> List[str]:
    """Descend the conference standings (best to worst) comparing vs each common
    opponent (or tied GROUP of opponents, compared collectively — the Big 12
    grouped-ties rule, adopted for all registry descent rungs per ambiguity 2).
    """
    others = [team for team in conf_pct_by_team if team not in cands]
    tiers: Dict[float, List[str]] = {}
    for team in others:
        tiers.setdefault(round(conf_pct_by_team[team], 9), []).append(team)
    for pct in sorted(tiers, reverse=True):
        group = tiers[pct]
        vs = {c: _pct_vs(rec_sim, c, group) for c in cands}
        if all(n > 0 for _, n in vs.values()):
            reduced = _keep_max(cands, {c: p / n for c, (p, n) in vs.items()})
            if len(reduced) < len(cands):
                return reduced
    return cands


def _apply_rung(
    cands: List[str],
    rung: _Rung,
    metrics: _Metrics,
    rec_sim: Dict[str, Dict[str, List[float]]],
    ctx: _TieCtx,
    rng: np.random.Generator,
) -> List[str]:
    kind = rung["kind"]
    if kind == "h2h":
        return _apply_h2h(cands, rec_sim, generic=bool(rung.get("generic")))
    if kind == "record_vs_common":
        return _apply_record_vs_common(cands, rec_sim)
    if kind == "record_vs_common_desc":
        return _apply_record_vs_common_desc(cands, rec_sim, ctx.conf_pct_by_team)
    if kind == "sov":
        return _keep_max(cands, {c: cast(float, metrics[c]["sov"]) for c in cands})
    if kind == "sos":
        return _keep_max(cands, {c: cast(float, metrics[c]["sos"]) for c in cands})
    if kind == "conf_pd":
        return _keep_max(cands, {c: cast(float, metrics[c]["conf_pd"]) for c in cands})
    if kind == "opp_conf_win_pct":
        return _keep_max(cands, {c: cast(float, metrics[c]["opp_wp_pooled"]) for c in cands})
    if kind == "capped_scoring_margin":
        margins = {c: metrics[c]["capped_margin"] for c in cands}
        if any(v is None for v in margins.values()):
            ctx.note(f"{ctx.conf_name}: capped_scoring_margin skipped (no home_points/away_points on games)")
            return cands
        return _keep_max(cands, cast(Dict[str, float], margins))
    if kind == "total_wins":
        if ctx.division_absent:
            ctx.note(
                f"{ctx.conf_name}: total_wins FCS cap not applied "
                "(no division column on teams; using uncapped win totals)"
            )
        return _keep_max(cands, {c: cast(float, metrics[c]["capped_wins"]) for c in cands})
    if kind == "analytics_rating":
        ratings = {c: metrics[c]["analytics_rating"] for c in cands}
        if any(v is None for v in ratings.values()):
            ctx.note(f"{ctx.conf_name}: analytics_rating skipped (no tiebreaker_data.analytics_ratings supplied)")
            return cands
        return _keep_max(cands, cast(Dict[str, float], ratings))
    if kind == "cfp_ranked_final_week":
        ranks = {c: metrics[c]["cfp_rank"] for c in cands}
        if all(v is None for v in ranks.values()):
            ctx.note(
                f"{ctx.conf_name}: cfp_ranked_final_week skipped "
                "(no tiebreaker_data.cfp_rankings supplied, or no tied team ranked)"
            )
            return cands
        # cands is sorted, so min() keeps the first of equal ranks (R's which.min)
        rank_key = {c: float("inf") if r is None else r for c, r in ranks.items()}
        best = min(cands, key=rank_key.__getitem__)
        if metrics[best]["final_conf_win"]:
            return [best]
        ctx.note(
            f"{ctx.conf_name}: cfp_ranked_final_week fell through "
            "(best-ranked tied team lost its final conference game)"
        )
        return cands
    if kind in ("apr", "div_pct"):
        vals = {c: metrics[c][kind] for c in cands}
        if any(v is None for v in vals.values()):
            why = "no tiebreaker_data.apr supplied" if kind == "apr" else "no conf_division column on teams"
            ctx.note(f"{ctx.conf_name}: {kind} skipped ({why})")
            return cands
        return _keep_max(cands, cast(Dict[str, float], vals))
    if kind == "coin_toss":
        return [str(rng.choice(sorted(cands)))]
    raise ValueError(f"unknown tiebreak rung kind: {kind!r}")


def _pick_winner(
    tied: List[str],
    metrics: _Metrics,
    rec_sim: Dict[str, Dict[str, List[float]]],
    rungs: List[_Rung],
    depth: int,
    rng: np.random.Generator,
    ctx: _TieCtx,
) -> str:
    """Reduce a tied set through ``rungs``; a trailing ``coin_toss`` rung always
    resolves whatever remains (both the generic cascade and every registry
    cascade end with one). ``min_depth`` (only present on generic rungs) gates
    by ``tiebreaker_depth`` exactly as before; registry rungs have no
    ``min_depth`` key and therefore always run (official procedures run in
    full — see the module docstring).
    """
    cands = sorted(tied)
    for rung in rungs:
        if len(cands) <= 1:
            break
        min_depth = rung.get("min_depth")
        if isinstance(min_depth, int) and depth < min_depth:
            continue
        cands = _apply_rung(cands, rung, metrics, rec_sim, ctx, rng)
    if len(cands) > 1:
        return str(rng.choice(sorted(cands)))
    return cands[0]


def _order_tied(
    tied: List[str],
    metrics: _Metrics,
    rec_sim: Dict[str, Dict[str, List[float]]],
    rungs: List[_Rung],
    depth: int,
    rng: np.random.Generator,
    ctx: _TieCtx,
) -> List[str]:
    """Peel one winner at a time from the tied set, restarting the full rung
    list on the shrinking remainder each time — this loop IS the "restart from
    rung 1 with the remaining set" rule for registry conferences (and is
    exactly the pre-registry generic behavior, unchanged).
    """
    ordered: List[str] = []
    remaining = sorted(tied)
    while len(remaining) > 1:
        winner = _pick_winner(remaining, metrics, rec_sim, rungs, depth, rng, ctx)
        ordered.append(winner)
        remaining.remove(winner)
    return ordered + remaining


def _order_set(
    team_set: List[str],
    pct_by_team: Dict[str, float],
    *,
    pool: Optional[str] = None,
    wl: Dict[str, Tuple[int, int]],
    metrics: _Metrics,
    rec_sim: _RecSim,
    rungs: List[_Rung],
    depth: int,
    rng: np.random.Generator,
    ctx_args: Tuple[str, bool, List[str]],
) -> List[str]:
    """Order ``team_set``: win-pct tiers (rounded to kill float fuzz), tiebreak
    walk inside each. ``pct_by_team`` is the descent-rung universe (the whole
    conference, or one division); ``wl`` maps team -> (conf wins, conf losses).
    """
    conf_name, division_absent, notes = ctx_args
    ctx = _TieCtx(conf_name, pct_by_team, division_absent, notes)
    pct = {t: round(pct_by_team[t], 9) for t in team_set}
    if pool == "wins_or_losses":
        # ACC 2026: alternate-game-count teams join the leaders' tier on matching
        # the leaders' win count OR loss count. A team with no decided conference
        # game is never admitted (cfbseedR 0.2.0 admits a 0-0 team on an
        # unbeaten leader's 0 losses — a bug there to fix in the R port).
        top = max(pct.values())
        lw = {wl[t][0] for t in team_set if pct[t] == top}
        ll = {wl[t][1] for t in team_set if pct[t] == top}
        for t in team_set:
            if pct[t] < top and sum(wl[t]) > 0 and (wl[t][0] in lw or wl[t][1] in ll):
                pct[t] = top
    ordered: List[str] = []
    for p in sorted(set(pct.values()), reverse=True):
        tier = [t for t in team_set if pct[t] == p]
        if len(tier) > 1:
            tier = _order_tied(tier, metrics, rec_sim, rungs, depth, rng, ctx)
        ordered += tier
    return ordered


def _add_conf_ranks(
    st: pl.DataFrame,
    dg_conf: pl.DataFrame,
    depth: int,
    rng: Optional[np.random.Generator],
    notes: List[str],
    division_absent: bool,
    season_of: Callable[[int], Optional[int]],
) -> pl.DataFrame:
    rec = _build_rec_map(dg_conf)
    if rng is None:
        rng = np.random.default_rng()
    non_ind = st.filter(_is_independent() == False)  # noqa: E712
    rank_rows: List[Tuple[int, str, int]] = []
    metric_map = {
        "sov": "sov",
        "sos": "sos",
        "conf_pd": "conf_pd",
        "opp_wp_pooled": "_opp_wp_pooled",
        "capped_margin": "_capped_margin",
        "capped_wins": "_capped_wins",
        "analytics_rating": "_analytics_rating",
        "cfp_rank": "_cfp_rank",
        "apr": "_apr",
        "div_pct": "_div_pct",
        "final_conf_win": "_final_conf_win",
    }
    extra = [c for c in ("conf_division", "postseason_eligible") if c in non_ind.columns]
    cols = ["sim", "conference", "team", "conf_pct", "conf_wins", "conf_losses", *metric_map.values(), *extra]
    # one row pass, grouped in python: per-(sim, conference) frame ops dominated 10k-sim runs
    groups: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
    for row in non_ind.select(cols).iter_rows(named=True):
        groups.setdefault((row["sim"], str(row["conference"])), []).append(row)
    for sim, conf_name in sorted(groups):
        rows = groups[(sim, conf_name)]
        rungs, pool = _conf_rules(conf_name, season_of(sim))
        rec_sim = rec.get(sim, {})
        metrics: _Metrics = {r["team"]: {k: r[v] for k, v in metric_map.items()} for r in rows}
        conf_pct = {r["team"]: r["conf_pct"] for r in rows}
        order_set = functools.partial(
            _order_set,
            wl={r["team"]: (r["conf_wins"], r["conf_losses"]) for r in rows},
            metrics=metrics,
            rec_sim=rec_sim,
            rungs=rungs,
            depth=depth,
            rng=rng,
            ctx_args=(conf_name, division_absent, notes),
        )
        teams_in = [r["team"] for r in rows]
        divs = {r["team"]: r["conf_division"] for r in rows} if "conf_division" in extra else {}
        if divs and None not in divs.values() and len(set(divs.values())) >= 2:
            # division format: each division ranked by the conference procedure,
            # the two champions take conf_rank 1-2 (ordered between themselves),
            # everyone else follows ranked across the whole conference
            by_div: Dict[str, List[str]] = {}
            for t in teams_in:
                by_div.setdefault(divs[t], []).append(t)
            champs = [order_set(m, {t: conf_pct[t] for t in m})[0] for _, m in sorted(by_div.items())]
            champs = order_set(champs, {t: conf_pct[t] for t in champs})
            ordered = champs + order_set([t for t in teams_in if t not in champs], conf_pct)
        else:
            ordered = order_set(teams_in, conf_pct, pool=pool)
        if "postseason_eligible" in extra:
            # stable partition: an explicitly ineligible team cannot hold a CCG berth
            out = {r["team"] for r in rows if r["postseason_eligible"] is False}
            ordered = [t for t in ordered if t not in out] + [t for t in ordered if t in out]
        rank_rows += [(sim, team, i) for i, team in enumerate(ordered, start=1)]
    rank_df = pl.DataFrame(
        rank_rows,
        schema={"sim": pl.Int64, "team": pl.Utf8, "conf_rank": pl.Int64},
        orient="row",
    )
    return st.join(rank_df, on=["sim", "team"], how="left")


def _add_conf_champ(st: pl.DataFrame, g: pl.DataFrame, t: pl.DataFrame) -> pl.DataFrame:
    """CONF_CHAMP game winner is champion; conferences without one fall back to conf_rank 1."""
    cc = (
        g.filter((pl.col("game_type") == "CONF_CHAMP") & pl.col("result").is_not_null() & (pl.col("result") != 0))
        .with_columns(
            pl.when(pl.col("result") > 0).then(pl.col("home_team")).otherwise(pl.col("away_team")).alias("champ_team")
        )
        .join(t.rename({"team": "champ_team"}), on="champ_team", how="left")
        .select("sim", "conference", "champ_team")
        .unique(subset=["sim", "conference"], keep="first", maintain_order=True)
    )
    return (
        st.join(cc, on=["sim", "conference"], how="left")
        .with_columns(
            pl.when(_is_independent())
            .then(False)
            .when(pl.col("champ_team").is_not_null())
            .then(pl.col("team") == pl.col("champ_team"))
            .otherwise(pl.col("conf_rank") == 1)
            .fill_null(False)
            .alias("conf_champ")
        )
        .drop("champ_team")
    )


def cfb_standings(
    games: FrameLike,
    teams: FrameLike,
    *,
    tiebreaker_depth: str = "SOS",
    playoff_seeds: Optional[int] = None,
    rankings: Optional[FrameLike] = None,
    tiebreaker_data: Optional[Dict[str, FrameLike]] = None,
    season: Optional[int] = None,
    return_as_pandas: bool = False,
    rng: Optional[np.random.Generator] = None,
) -> Union[pl.DataFrame, Any]:
    """Compute college football standings with conference ranks and champions.

    Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe);
    see the module docstring for the documented CFB simplifications, and its
    "Official per-conference tiebreakers (registry)" section for how
    ``CONFERENCE_TIEBREAKERS`` overrides the generic cascade for the SEC,
    Big Ten, Big 12, ACC, MAC, American, Conference USA, Mountain West and
    Sun Belt.

    Args:
        games: Game results with columns ``sim`` (or ``season``), ``week``,
            ``game_type`` (``REG`` | ``CONF_CHAMP`` | ``POST``), ``home_team``,
            ``away_team``, ``result`` (home margin: home - away; null =
            unplayed), optional ``neutral`` (0/1), and optional
            ``home_points``/``away_points`` (per-game scores — feeds the SEC
            capped-scoring-margin rung; :func:`cfb_games_from_schedule`
            emits both). Either optional input absent -> that rung is
            skipped, not an error.
        teams: Team table with columns ``team`` and ``conference``
            (null or ``"FBS Independents"`` marks an independent), and an
            optional ``division`` column (``"FBS"``/``"FCS"`` or similar —
            feeds the Big 12 ``total_wins`` FCS cap; absent -> the cap
            degrades to uncapped win totals, noted in ``tiebreak_notes``).
            Optional ``conf_division`` (in-conference division name: with two
            or more divisions the division champions take ``conf_rank`` 1-2,
            and it feeds the Sun Belt ``div_pct`` rung) and
            ``postseason_eligible`` (bool; an explicit ``False`` team is
            ranked behind every eligible one so it cannot hold a
            championship-game berth, while its games still count).
        tiebreaker_depth: One of ``"RANDOM"``, ``"PRE-SOV"``, ``"SOS"``,
            ``"POINTS"`` — the nflseedR depth ladder. Steps beyond the chosen
            depth are skipped and remaining ties are broken by coin flip.
            Gates ONLY the generic fallback cascade; registered official
            conference procedures (below) always run in full.
        playoff_seeds: If set, adds a ``seed`` column via
            :func:`cfb_playoff_seeds` with this field size (the season's CFP
            selection rule, resolved like the tiebreaker epochs).
        rankings: Optional committee-style rankings frame (``team``, ``rank``)
            forwarded to :func:`cfb_playoff_seeds`.
        tiebreaker_data: Optional external inputs for the registry rungs, as
            a dict of frames: ``"analytics_ratings"`` (``team``, ``rating`` —
            the ``analytics_rating`` rung: Big Ten/Big 12/ACC/MAC, and the
            American/CUSA/Mountain West/Sun Belt metric composites),
            ``"cfp_rankings"`` (``team``, ``rank`` — the
            ``cfp_ranked_final_week`` clause: American/CUSA/Sun Belt) and
            ``"apr"`` (``team``, ``apr`` — the CUSA ``apr`` rung). Missing ->
            the rung is skipped, noted.
        season: Season whose rules apply (registry epochs and the CFP
            selection rule) to every sim. ``None`` (default): when ``games``
            is keyed by ``season`` each id is its own season, otherwise
            (plain ``sim`` ids) the current rules apply.
        return_as_pandas: Return a pandas DataFrame instead of polars.
        rng: Optional numpy Generator used only for coin-flip tiebreaks
            (simulations pass their seeded generator through here).

    Returns:
        A polars (or pandas) DataFrame with one row per (sim, team):
        overall record (``games``/``wins``/``losses``/``ties``/``win_pct``/
        ``pd``), conference record (``conf_*``), ``sov``, ``sos``,
        ``conf_rank`` (null for independents), ``conf_champ`` and, when
        ``playoff_seeds`` is set, ``seed``. The result also carries a
        ``tiebreak_notes`` list of skipped-rung messages (see the module
        docstring): ``result.tiebreak_notes`` for a polars frame,
        ``result.attrs["tiebreak_notes"]`` for a pandas frame (pandas' own
        metadata mechanism — avoids its "new attribute" warning).

    Raises:
        ValueError: If required columns are missing or ``tiebreaker_depth`` /
            ``game_type`` values are invalid.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import cfb_standings

            games = pl.DataFrame({
                "sim": [2024, 2024], "week": [1, 2],
                "game_type": ["REG", "REG"],
                "home_team": ["A", "B"], "away_team": ["B", "A"],
                "result": [7.0, -3.0], "neutral": [0, 0],
            })
            teams = pl.DataFrame({"team": ["A", "B"], "conference": ["X", "X"]})
            print(cfb_standings(games, teams))

        With CFP seeds from committee rankings::

            st = cfb_standings(games, teams, playoff_seeds=12, rankings=ranks_df)

        With an official-registry analytics rating input::

            ratings = pl.DataFrame({"team": ["A", "B"], "rating": [92.1, 88.4]})
            st = cfb_standings(games, teams, tiebreaker_data={"analytics_ratings": ratings})
            print(st.tiebreak_notes)

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the engine this adapts.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if tiebreaker_depth not in TIEBREAKER_DEPTHS:
        raise ValueError(f"`tiebreaker_depth` must be one of {sorted(TIEBREAKER_DEPTHS)}; got {tiebreaker_depth!r}")
    depth = TIEBREAKER_DEPTHS[tiebreaker_depth]
    season_keyed = "sim" not in list(games.columns) and "season" in list(games.columns)
    g = _validate_games(games)
    t = _validate_teams(teams)
    st, dg_conf = _standings_base(g, t)
    tb = tiebreaker_data or {}
    for key, src, dst in (
        ("analytics_ratings", "rating", "_analytics_rating"),
        ("cfp_rankings", "rank", "_cfp_rank"),
        ("apr", "apr", "_apr"),
    ):
        if key in tb:
            # NaN -> null so a missing value skips the rung (R's anyNA) instead of breaking _keep_max
            ext = _to_polars(tb[key]).select(
                pl.col("team").cast(pl.Utf8), pl.col(src).cast(pl.Float64).fill_nan(None).alias(dst)
            )
            st = st.join(ext, on="team", how="left")
        else:
            st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias(dst))
    # whose rules apply: explicit season > season-keyed ids (each sim IS a season) > current
    season_of: Callable[[int], Optional[int]]
    if season is not None:
        season_of = lambda _sim: season  # noqa: E731
    elif season_keyed:
        season_of = lambda sim: sim  # noqa: E731
    else:
        season_of = lambda _sim: None  # noqa: E731
    division_absent = "division" not in t.columns
    notes: List[str] = []
    st = _add_conf_ranks(st, dg_conf, depth, rng, notes, division_absent, season_of)
    st = _add_conf_champ(st, g, t)
    st = st.select(
        "sim",
        "team",
        "conference",
        "games",
        "wins",
        "losses",
        "ties",
        "win_pct",
        "pd",
        "conf_games",
        "conf_wins",
        "conf_losses",
        "conf_ties",
        "conf_pct",
        "conf_pd",
        "sov",
        "sos",
        "conf_rank",
        "conf_champ",
    )
    if playoff_seeds is not None:
        rule_2026 = (
            pl.lit(season >= 2026) if season is not None else (pl.col("sim") >= 2026 if season_keyed else pl.lit(True))
        )
        st = _seed_field(st, rankings, playoff_seeds, rule_2026)
    st = st.sort(["sim", "conference", "conf_rank", "team"], nulls_last=True)
    if return_as_pandas:
        pdf = st.to_pandas(use_pyarrow_extension_array=True)
        pdf.attrs["tiebreak_notes"] = notes
        return pdf
    st.tiebreak_notes = notes
    return st


# 2026 CFP automatic qualifiers, matched against cfbfastR/cfbd and short
# spellings (same lists as cfbseedR 0.2.0's cfb_playoff_seeds).
_P4_CONFS = (
    "SEC",
    "Southeastern Conference",
    "Big Ten",
    "Big Ten Conference",
    "ACC",
    "Atlantic Coast Conference",
    "Big 12",
    "Big 12 Conference",
)
_G6_CONFS = (
    "American Athletic",
    "American",
    "American Athletic Conference",
    "American Conference",
    "Conference USA",
    "CUSA",
    "MAC",
    "Mid-American",
    "Mid-American Conference",
    "Mountain West",
    "Mountain West Conference",
    "Pac-12",
    "Pac-12 Conference",
    "Sun Belt",
    "Sun Belt Conference",
)
# Notre Dame by display name or ESPN team id
_NOTRE_DAME = ("Notre Dame", "87")


def _seed_field(
    standings: pl.DataFrame,
    rankings: Optional[FrameLike],
    playoff_seeds: int,
    rule_2026: pl.Expr,
) -> pl.DataFrame:
    """Select and straight-seed the field per sim; ``rule_2026`` (bool expr) picks the rule per row."""
    st = standings.drop("seed") if "seed" in standings.columns else standings
    if rankings is not None:
        r = _to_polars(rankings).select(pl.col("team").cast(pl.Utf8), pl.col("rank").cast(pl.Float64).fill_nan(None))
        st = st.join(r, on="team", how="left")
    else:
        st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias("rank"))
    n = pl.lit(playoff_seeds, dtype=pl.Int64)
    is_champ = pl.col("conf_champ") == True  # noqa: E712
    g6_champ = is_champ & pl.col("conference").is_in(_G6_CONFS)
    ordered = (
        st.with_columns(pl.col("rank").fill_null(float("inf")))
        .sort(
            ["sim", "rank", "win_pct", "sov", "sos", "pd", "team"],
            descending=[False, False, True, True, True, True, False],
        )
        .with_columns(
            pl.int_range(1, pl.len() + 1).over("sim").alias("_pos"),
            is_champ.cast(pl.Int64).cum_sum().over("sim").alias("_champ_cum"),
            g6_champ.cast(pl.Int64).cum_sum().over("sim").alias("_g6_cum"),
        )
        .with_columns(
            pl.when(rule_2026)
            # 2026: P4 champions regardless of rank, the best-ranked G6 champion,
            # Notre Dame when it ranks inside the field size
            .then(
                (is_champ & pl.col("conference").is_in(_P4_CONFS))
                | (g6_champ & (pl.col("_g6_cum") == 1))
                | (pl.col("team").is_in(_NOTRE_DAME) & (pl.col("_pos") <= n))
            )
            # 2024-2025: the 5 highest-ranked conference champions
            .otherwise(is_champ & (pl.col("_champ_cum") <= 5))
            .alias("_auto")
        )
        .with_columns((pl.col("_auto") & (pl.col("_auto").cast(pl.Int64).cum_sum().over("sim") <= n)).alias("_auto"))
        .with_columns(
            (pl.col("_auto") == False).cast(pl.Int64).cum_sum().over("sim").alias("_al_cum"),  # noqa: E712
            pl.col("_auto").cast(pl.Int64).sum().over("sim").alias("_n_auto"),
        )
        .with_columns((pl.col("_auto") | (pl.col("_al_cum") <= n - pl.col("_n_auto"))).alias("_in_field"))
        # straight seeding in ranking order: an automatic qualifier ranked outside
        # the top `playoff_seeds` sits behind every at-large, so it takes the lowest seeds
        .with_columns(
            pl.when(pl.col("_in_field"))
            .then(pl.col("_in_field").cast(pl.Int64).cum_sum().over("sim"))
            .otherwise(None)
            .cast(pl.Int64)
            .alias("seed")
        )
        .drop("rank", "_pos", "_champ_cum", "_g6_cum", "_auto", "_al_cum", "_n_auto", "_in_field")
        .sort(["sim", "seed"], nulls_last=True)
    )
    return ordered


def cfb_playoff_seeds(
    standings: FrameLike,
    rankings: Optional[FrameLike] = None,
    playoff_seeds: int = 12,
    *,
    season: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, Any]:
    """Assign College Football Playoff seeds (12-team straight seeding, by season).

    Two selection rules, picked by ``season``; both seed the field straight by
    ranking order (no champion bump to the top four):

    * **2026+** (``season=None`` or ``>= 2026``, the current rule): the ACC,
      Big Ten, Big 12 and SEC champions are in regardless of ranking; so is
      the highest-ranked champion of the American, Conference USA, MAC,
      Mountain West, Pac-12 and Sun Belt; and Notre Dame (``"Notre Dame"`` or
      ESPN id ``"87"``) when it ranks inside the top ``playoff_seeds``; the
      rest of the field is at-large.
    * **2024-2025** (``season < 2026``): the 5 highest-ranked conference
      champions are guaranteed inclusion, the rest is at-large.

    An automatic qualifier ranked outside the top ``playoff_seeds`` displaces
    the lowest-ranked at-large team and takes the lowest seed. Conference
    names match both cfbd and short spellings (e.g. ``"American Athletic"`` /
    ``"American"``).

    When ``rankings`` is None the ordering falls back to the standings
    tiebreaker metrics — ``win_pct`` desc, then ``sov``, ``sos``, ``pd``
    desc, then team name (documented deterministic fallback; a committee
    ranking is the intended input). Notre Dame's "ranked inside the field"
    test reads its position in that same ordering.

    Differs from cfbseedR 0.2.0 (``autobid``) in three places: cfbseedR gives
    the Group-of-6 bid to the best-ranked G6 team whether or not it won its
    conference; it tests Notre Dame's committee ``rank`` only (no bid without
    ``rankings``); and it matches ``"Notre Dame"`` but not ``"87"``. Its
    ``cfb_standings()`` / ``cfb_simulations()`` also always apply the current
    rules, where :func:`cfb_standings` and
    :func:`sportsdataverse.cfb.cfb_simulations` here follow a
    ``season``-keyed input.

    Args:
        standings: Output of :func:`cfb_standings` (needs ``sim``, ``team``,
            ``conference``, ``conf_champ``, ``win_pct``, ``sov``, ``sos``,
            ``pd``).
        rankings: Optional frame with columns ``team`` and ``rank`` (1 =
            best). Unranked teams order after ranked ones by the fallback.
        playoff_seeds: Field size (default 12). Automatic qualifiers are
            capped at this size.
        season: Season whose selection rule applies (default ``None`` = the
            current, 2026 rule).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The standings frame with a ``seed`` column (Int64; null for teams
        outside the field), sorted by sim and seed.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_standings, cfb_playoff_seeds
            st = cfb_standings(games, teams)
            seeded = cfb_playoff_seeds(st, rankings=ranks_df, playoff_seeds=12)
            print(seeded.filter(pl.col("seed").is_not_null()))

        The 2024-2025 rule (5 highest-ranked champions)::

            seeded = cfb_playoff_seeds(st, rankings=ranks_df, season=2025)

    See Also:
        * `cfbseedR <https://cfbseedR.sportsdataverse.org>`_ -- the R port (``autobid``).
        * `nflseedR <https://nflseedr.com>`_ -- NFL seeding equivalent.
    """
    rule_2026 = pl.lit(season is None or season >= 2026)
    ordered = _seed_field(_to_polars(standings), rankings, playoff_seeds, rule_2026)
    return ordered.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else ordered


def cfb_games_from_schedule(schedule: FrameLike, *, return_as_pandas: bool = False) -> Union[pl.DataFrame, Any]:
    """Map a ``load_cfb_schedule()`` frame into the seedr engine ``games`` schema.

    Derives ``game_type`` heuristically: games whose ``notes`` mention a
    championship (but not the CFP / national championship) are
    ``CONF_CHAMP``; otherwise ``season_type == "regular"`` maps to ``REG``
    and everything else to ``POST``. ``result`` is the home margin
    (``home_points - away_points``; null when either score is missing) and
    ``neutral`` comes from ``neutral_site``.

    Args:
        schedule: Output of :func:`sportsdataverse.cfb.load_cfb_schedule`
            (needs ``season``, ``week``, ``season_type``, ``home_team``,
            ``away_team``, ``home_points``, ``away_points``, ``neutral_site``
            and optionally ``notes``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame with columns ``season``, ``week``,
        ``game_type``, ``home_team``, ``away_team``, ``result``, ``neutral``,
        ``home_points``, ``away_points`` — the :func:`cfb_standings` /
        :func:`cfb_simulations` input schema. The trailing per-game points
        columns feed the SEC ``capped_scoring_margin`` official tiebreaker
        rung (see :data:`CONFERENCE_TIEBREAKERS`); :func:`cfb_standings`
        skips that rung when they're absent, so passing this frame straight
        through is always safe.

    Example:
        Loader to standings pipeline::

            import polars as pl
            from sportsdataverse.cfb import (
                load_cfb_schedule, cfb_games_from_schedule, cfb_standings,
            )

            sched = load_cfb_schedule(seasons=2024)
            games = cfb_games_from_schedule(sched)
            teams = (
                sched.select(team=pl.col("home_team"), conference=pl.col("home_conference"))
                .vstack(sched.select(team=pl.col("away_team"), conference=pl.col("away_conference")))
                .unique(subset=["team"], keep="first")
            )
            st = cfb_standings(games, teams)
            print(st.head())

    See Also:
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- same schedule shape in R.
        * `nflseedR <https://nflseedr.com>`_ -- the engine this feeds.
    """
    s = _to_polars(schedule)
    notes = pl.col("notes").cast(pl.Utf8) if "notes" in s.columns else pl.lit(None, dtype=pl.Utf8)
    is_conf_champ = (
        notes.str.contains("(?i)championship").fill_null(False)
        & ~notes.str.contains("(?i)college football playoff").fill_null(False)
        & ~notes.str.contains("(?i)national championship").fill_null(False)
    )
    out = s.select(
        pl.col("season").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        pl.when(is_conf_champ)
        .then(pl.lit("CONF_CHAMP"))
        .when(pl.col("season_type").cast(pl.Utf8).str.to_lowercase() == "regular")
        .then(pl.lit("REG"))
        .otherwise(pl.lit("POST"))
        .alias("game_type"),
        pl.col("home_team").cast(pl.Utf8),
        pl.col("away_team").cast(pl.Utf8),
        (pl.col("home_points").cast(pl.Float64) - pl.col("away_points").cast(pl.Float64)).alias("result"),
        pl.col("neutral_site").cast(pl.Int64).fill_null(0).alias("neutral"),
        pl.col("home_points").cast(pl.Float64),
        pl.col("away_points").cast(pl.Float64),
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
