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
  registers the official 2024+ procedures for the SEC, Big Ten, Big 12, ACC
  and MAC as plain rung-dict lists; any conference not in the registry uses
  the generic cascade above, completely unchanged. Registry cascades always
  run in full (``tiebreaker_depth`` gates only the generic fallback).
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
  ``division`` flag on ``teams``, analytics ratings) are skipped
  deterministically; each skip is recorded in ``tiebreak_notes`` (see
  :func:`cfb_standings`). Big 12 NCAA-exempt games are not modeled.
* **CFP seeding rule evolves.** :func:`cfb_playoff_seeds` implements the
  current (2025) straight-seeding 12-team rule in ONE function so the rule
  can be updated in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast

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

#: Official 2024+ conference tiebreaker procedures as plain rung-dict lists
#: (mirrored as a named list in the R ``cfbseedR`` port; the cross-language
#: parity fixture under ``tests/fixtures/seedr/cfb_toy_tiebreakers/`` pins the
#: semantics). Conferences not registered here use the generic cascade. G5
#: conferences intentionally stay on the fallback in v1 — their published
#: procedures depend on unspecified external metric composites.
CONFERENCE_TIEBREAKERS: Dict[str, List[_Rung]] = {
    "SEC": [
        {"kind": "h2h"},
        {"kind": "record_vs_common"},
        {"kind": "record_vs_common_desc", "mode": "order_of_finish", "grouped_ties": True},
        {"kind": "opp_conf_win_pct"},
        {"kind": "capped_scoring_margin", "off_cap": 42, "def_cap": 48},
        {"kind": "coin_toss"},
    ],
    "Big Ten": _P5_TEMPLATE,
    "ACC": _P5_TEMPLATE,
    "MAC": _P5_TEMPLATE,
    "Mid-American": _P5_TEMPLATE,  # cfbfastR schedule naming for the MAC
    "Big 12": [
        {"kind": "h2h"},
        {"kind": "record_vs_common"},
        {"kind": "record_vs_common_desc", "mode": "next_highest", "grouped_ties": True},
        {"kind": "opp_conf_win_pct"},
        {"kind": "total_wins", "fcs_cap": 1},
        {"kind": "analytics_rating"},
        {"kind": "coin_toss"},
    ],
}


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


def _add_conf_ranks(
    st: pl.DataFrame,
    dg_conf: pl.DataFrame,
    depth: int,
    rng: Optional[np.random.Generator],
    notes: List[str],
    division_absent: bool,
) -> pl.DataFrame:
    rec = _build_rec_map(dg_conf)
    non_ind = st.filter(_is_independent() == False)  # noqa: E712
    rank_rows: List[Tuple[int, str, int]] = []
    lazy_rng: Optional[np.random.Generator] = rng
    metric_cols = (
        "team",
        "conf_pct",
        "sov",
        "sos",
        "conf_pd",
        "_opp_wp_pooled",
        "_capped_margin",
        "_capped_wins",
        "_analytics_rating",
    )
    parts = non_ind.partition_by(["sim", "conference"], as_dict=True)
    for key in sorted(parts, key=lambda k: (k[0], str(k[1]))):
        grp = parts[key]
        sim = key[0]
        conf_name = str(key[1])
        rungs = CONFERENCE_TIEBREAKERS.get(conf_name, _GENERIC_CASCADE)
        metrics: _Metrics = {}
        conf_pct_by_team: Dict[str, float] = {}
        for row in grp.select(*metric_cols).iter_rows(named=True):
            team = row["team"]
            conf_pct_by_team[team] = row["conf_pct"]
            metrics[team] = {
                "conf_pct": row["conf_pct"],
                "sov": row["sov"],
                "sos": row["sos"],
                "conf_pd": row["conf_pd"],
                "opp_wp_pooled": row["_opp_wp_pooled"],
                "capped_margin": row["_capped_margin"],
                "capped_wins": row["_capped_wins"],
                "analytics_rating": row["_analytics_rating"],
            }
        ctx = _TieCtx(
            conf_name=conf_name,
            conf_pct_by_team=conf_pct_by_team,
            division_absent=division_absent,
            notes=notes,
        )
        # tiers by conference win pct (primary sort), rounded to kill float fuzz
        tiers: Dict[float, List[str]] = {}
        for team, m in metrics.items():
            tiers.setdefault(round(cast(float, m["conf_pct"]), 9), []).append(team)
        rank = 1
        for pct in sorted(tiers, reverse=True):
            tier = tiers[pct]
            if len(tier) > 1:
                if lazy_rng is None:
                    lazy_rng = np.random.default_rng()
                tier = _order_tied(tier, metrics, rec.get(sim, {}), rungs, depth, lazy_rng, ctx)
            for team in tier:
                rank_rows.append((sim, team, rank))
                rank += 1
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
    return_as_pandas: bool = False,
    rng: Optional[np.random.Generator] = None,
) -> Union[pl.DataFrame, Any]:
    """Compute college football standings with conference ranks and champions.

    Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe);
    see the module docstring for the documented CFB simplifications, and its
    "Official per-conference tiebreakers (registry)" section for how
    ``CONFERENCE_TIEBREAKERS`` overrides the generic cascade for the SEC,
    Big Ten, Big 12, ACC and MAC.

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
        tiebreaker_depth: One of ``"RANDOM"``, ``"PRE-SOV"``, ``"SOS"``,
            ``"POINTS"`` — the nflseedR depth ladder. Steps beyond the chosen
            depth are skipped and remaining ties are broken by coin flip.
            Gates ONLY the generic fallback cascade; registered official
            conference procedures (below) always run in full.
        playoff_seeds: If set, adds a ``seed`` column via
            :func:`cfb_playoff_seeds` with this field size.
        rankings: Optional committee-style rankings frame (``team``, ``rank``)
            forwarded to :func:`cfb_playoff_seeds`.
        tiebreaker_data: Optional external inputs for the registry rungs, as
            a dict with key ``"analytics_ratings"`` -> a frame with columns
            ``team`` and ``rating`` (feeds the ``analytics_rating`` rung
            used by Big Ten/Big 12/ACC/MAC). A ``"cfp_rankings"`` key
            (``team``, ``rank``) is accepted for forward compatibility but
            unused by the current registry (no registered conference has a
            ``cfp_ranking`` rung yet). Missing -> the rung is skipped, noted.
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
    g = _validate_games(games)
    t = _validate_teams(teams)
    st, dg_conf = _standings_base(g, t)
    if tiebreaker_data is not None and "analytics_ratings" in tiebreaker_data:
        ar = _to_polars(tiebreaker_data["analytics_ratings"]).select(
            pl.col("team").cast(pl.Utf8), pl.col("rating").cast(pl.Float64).alias("_analytics_rating")
        )
        st = st.join(ar, on="team", how="left")
    else:
        st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias("_analytics_rating"))
    division_absent = "division" not in t.columns
    notes: List[str] = []
    st = _add_conf_ranks(st, dg_conf, depth, rng, notes, division_absent)
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
        st = cfb_playoff_seeds(st, rankings=rankings, playoff_seeds=playoff_seeds)
    st = st.sort(["sim", "conference", "conf_rank", "team"], nulls_last=True)
    if return_as_pandas:
        pdf = st.to_pandas(use_pyarrow_extension_array=True)
        pdf.attrs["tiebreak_notes"] = notes
        return pdf
    st.tiebreak_notes = notes
    return st


def cfb_playoff_seeds(
    standings: FrameLike,
    rankings: Optional[FrameLike] = None,
    playoff_seeds: int = 12,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, Any]:
    """Assign College Football Playoff seeds (current straight-seeding rule).

    Implements the 2025 CFP rule: the field is the ``playoff_seeds`` (12)
    best-ranked teams with the 5 highest-ranked conference champions
    guaranteed inclusion; seeds are assigned straight by ranking order (no
    champion bump to the top four). The rule evolves — it lives in this ONE
    function so it can be updated in one place.

    When ``rankings`` is None the ordering falls back to the standings
    tiebreaker metrics — ``win_pct`` desc, then ``sov``, ``sos``, ``pd``
    desc, then team name (documented deterministic fallback; a committee
    ranking is the intended input).

    Args:
        standings: Output of :func:`cfb_standings` (needs ``sim``, ``team``,
            ``conf_champ``, ``win_pct``, ``sov``, ``sos``, ``pd``).
        rankings: Optional frame with columns ``team`` and ``rank`` (1 =
            best). Unranked teams order after ranked ones by the fallback.
        playoff_seeds: Field size (default 12). The champion guarantee is
            ``min(5, number of champions, playoff_seeds)``.
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

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- NFL seeding equivalent.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    st = _to_polars(standings)
    if "seed" in st.columns:
        st = st.drop("seed")
    if rankings is not None:
        r = _to_polars(rankings).select(pl.col("team").cast(pl.Utf8), pl.col("rank").cast(pl.Float64))
        st = st.join(r, on="team", how="left")
    else:
        st = st.with_columns(pl.lit(None, dtype=pl.Float64).alias("rank"))
    ordered = st.with_columns(pl.col("rank").fill_null(float("inf"))).sort(
        ["sim", "rank", "win_pct", "sov", "sos", "pd", "team"],
        descending=[False, False, True, True, True, True, False],
    )
    is_champ = pl.col("conf_champ") == True  # noqa: E712
    ordered = (
        ordered.with_columns(
            is_champ.cast(pl.Int64).cum_sum().over("sim").alias("_champ_cum"),
            is_champ.cast(pl.Int64).sum().over("sim").alias("_champ_total"),
        )
        .with_columns(
            pl.min_horizontal(
                pl.lit(5, dtype=pl.Int64), pl.col("_champ_total"), pl.lit(playoff_seeds, dtype=pl.Int64)
            ).alias("_n_guar")
        )
        .with_columns((is_champ & (pl.col("_champ_cum") <= pl.col("_n_guar"))).alias("_guaranteed"))
        .with_columns(
            (pl.col("_guaranteed") == False)  # noqa: E712
            .cast(pl.Int64)
            .cum_sum()
            .over("sim")
            .alias("_al_cum")
        )
        .with_columns(
            (
                pl.col("_guaranteed")
                | (pl.col("_al_cum") <= (pl.lit(playoff_seeds, dtype=pl.Int64) - pl.col("_n_guar")))
            ).alias("_in_field")
        )
        .with_columns(
            pl.when(pl.col("_in_field") == True)  # noqa: E712
            .then(pl.col("_in_field").cast(pl.Int64).cum_sum().over("sim"))
            .otherwise(None)
            .cast(pl.Int64)
            .alias("seed")
        )
        .drop("rank", "_champ_cum", "_champ_total", "_n_guar", "_guaranteed", "_al_cum", "_in_field")
        .sort(["sim", "seed"], nulls_last=True)
    )
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
