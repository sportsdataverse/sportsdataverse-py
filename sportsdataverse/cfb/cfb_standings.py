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
* **CFP seeding rule evolves.** :func:`cfb_playoff_seeds` implements the
  current (2025) straight-seeding 12-team rule in ONE function so the rule
  can be updated in one place.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import polars as pl

__all__ = ["cfb_standings", "cfb_playoff_seeds", "cfb_games_from_schedule"]

TIEBREAKER_DEPTHS: Dict[str, int] = {"RANDOM": 0, "PRE-SOV": 1, "SOS": 2, "POINTS": 3}
INDEPENDENT_LABEL = "FBS Independents"
_VALID_GAME_TYPES = ("REG", "CONF_CHAMP", "POST")

# rec map: sim -> team -> opp -> [outcome_points, n_games] (conference REG games only)
_RecMap = Dict[Any, Dict[str, Dict[str, List[float]]]]
# per-team cascade metrics: (conf_pct, sov, sos, conf_pd)
_Metrics = Dict[str, Tuple[float, float, float, float]]

FrameLike = Union[pl.DataFrame, Any]


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
    t = t.select(pl.col("team").cast(pl.Utf8), pl.col("conference").cast(pl.Utf8))
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
    return g.select(
        pl.col("sim").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        pl.col("game_type").cast(pl.Utf8),
        pl.col("home_team").cast(pl.Utf8),
        pl.col("away_team").cast(pl.Utf8),
        pl.col("result").cast(pl.Float64),
        pl.col("neutral").cast(pl.Int64),
    )


def _double_games(g: pl.DataFrame) -> pl.DataFrame:
    """Long form: one row per (game, perspective team), played games only."""
    home = g.select(
        "sim",
        "week",
        "game_type",
        pl.col("home_team").alias("team"),
        pl.col("away_team").alias("opp"),
        pl.col("result"),
    )
    away = g.select(
        "sim",
        "week",
        "game_type",
        pl.col("away_team").alias("team"),
        pl.col("home_team").alias("opp"),
        (-pl.col("result")).alias("result"),
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
    )
    sovsos = (
        dgc.join(conf_pct_opp, on=["sim", "opp"], how="left")
        .with_columns(won=(pl.col("outcome") == 1.0).cast(pl.Float64))
        .group_by("sim", "team")
        .agg(
            pl.col("_opp_conf_pct").mean().cast(pl.Float64).alias("sos"),
            (pl.col("_opp_conf_pct") * pl.col("won")).sum().alias("sov_num"),
            pl.col("won").sum().alias("sov_den"),
        )
        .with_columns(
            pl.when(pl.col("sov_den") > 0)
            .then(pl.col("sov_num") / pl.col("sov_den"))
            .otherwise(0.0)
            .cast(pl.Float64)
            .alias("sov")
        )
        .select("sim", "team", "sov", "sos")
    )

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
            pl.col("pd", "conf_pd", "sov", "sos").fill_null(0.0),
        )
        .with_columns(
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


def _pick_winner(
    tied: List[str],
    metrics: _Metrics,
    rec_sim: Dict[str, Dict[str, List[float]]],
    depth: int,
    rng: np.random.Generator,
) -> str:
    """Reduce a tied set through the cascade; coin-flip whatever remains."""
    cands = sorted(tied)
    if depth >= 1 and len(cands) > 1:
        # head-to-head among the tied set (conference REG games); skipped if any
        # candidate has no game against the set (documented simplification).
        h2h = {c: _pct_vs(rec_sim, c, [x for x in cands if x != c]) for c in cands}
        if all(n > 0 for _, n in h2h.values()):
            cands = _keep_max(cands, {c: p / n for c, (p, n) in h2h.items()})
        if len(cands) > 1:
            # common conference opponents (min 1), excluding the tied set itself
            opp_sets = [set(rec_sim.get(c, {})) for c in cands]
            commons = set.intersection(*opp_sets) - set(cands) if opp_sets else set()
            if commons:
                vs = {c: _pct_vs(rec_sim, c, sorted(commons)) for c in cands}
                cands = _keep_max(cands, {c: p / n for c, (p, n) in vs.items()})
    if depth >= 2 and len(cands) > 1:
        cands = _keep_max(cands, {c: metrics[c][1] for c in cands})  # sov (conference-scoped)
        if len(cands) > 1:
            cands = _keep_max(cands, {c: metrics[c][2] for c in cands})  # sos (conference-scoped)
    if depth >= 3 and len(cands) > 1:
        cands = _keep_max(cands, {c: metrics[c][3] for c in cands})  # conference point diff
    if len(cands) > 1:
        return str(rng.choice(sorted(cands)))
    return cands[0]


def _order_tied(
    tied: List[str],
    metrics: _Metrics,
    rec_sim: Dict[str, Dict[str, List[float]]],
    depth: int,
    rng: np.random.Generator,
) -> List[str]:
    ordered: List[str] = []
    remaining = sorted(tied)
    while len(remaining) > 1:
        winner = _pick_winner(remaining, metrics, rec_sim, depth, rng)
        ordered.append(winner)
        remaining.remove(winner)
    return ordered + remaining


def _add_conf_ranks(
    st: pl.DataFrame,
    dg_conf: pl.DataFrame,
    depth: int,
    rng: Optional[np.random.Generator],
) -> pl.DataFrame:
    rec = _build_rec_map(dg_conf)
    non_ind = st.filter(_is_independent() == False)  # noqa: E712
    rank_rows: List[Tuple[int, str, int]] = []
    lazy_rng: Optional[np.random.Generator] = rng
    parts = non_ind.partition_by(["sim", "conference"], as_dict=True)
    for key in sorted(parts, key=lambda k: (k[0], str(k[1]))):
        grp = parts[key]
        sim = key[0]
        metrics: _Metrics = {}
        for row in grp.select("team", "conf_pct", "sov", "sos", "conf_pd").iter_rows():
            metrics[row[0]] = (row[1], row[2], row[3], row[4])
        # tiers by conference win pct (primary sort), rounded to kill float fuzz
        tiers: Dict[float, List[str]] = {}
        for team, m in metrics.items():
            tiers.setdefault(round(m[0], 9), []).append(team)
        rank = 1
        for pct in sorted(tiers, reverse=True):
            tier = tiers[pct]
            if len(tier) > 1:
                if lazy_rng is None:
                    lazy_rng = np.random.default_rng()
                tier = _order_tied(tier, metrics, rec.get(sim, {}), depth, lazy_rng)
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
    return_as_pandas: bool = False,
    rng: Optional[np.random.Generator] = None,
) -> Union[pl.DataFrame, Any]:
    """Compute college football standings with conference ranks and champions.

    Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe);
    see the module docstring for the documented CFB simplifications.

    Args:
        games: Game results with columns ``sim`` (or ``season``), ``week``,
            ``game_type`` (``REG`` | ``CONF_CHAMP`` | ``POST``), ``home_team``,
            ``away_team``, ``result`` (home margin: home - away; null =
            unplayed) and optional ``neutral`` (0/1).
        teams: Team table with columns ``team`` and ``conference``
            (null or ``"FBS Independents"`` marks an independent).
        tiebreaker_depth: One of ``"RANDOM"``, ``"PRE-SOV"``, ``"SOS"``,
            ``"POINTS"`` — the nflseedR depth ladder. Steps beyond the chosen
            depth are skipped and remaining ties are broken by coin flip.
        playoff_seeds: If set, adds a ``seed`` column via
            :func:`cfb_playoff_seeds` with this field size.
        rankings: Optional committee-style rankings frame (``team``, ``rank``)
            forwarded to :func:`cfb_playoff_seeds`.
        return_as_pandas: Return a pandas DataFrame instead of polars.
        rng: Optional numpy Generator used only for coin-flip tiebreaks
            (simulations pass their seeded generator through here).

    Returns:
        A polars (or pandas) DataFrame with one row per (sim, team):
        overall record (``games``/``wins``/``losses``/``ties``/``win_pct``/
        ``pd``), conference record (``conf_*``), ``sov``, ``sos``,
        ``conf_rank`` (null for independents), ``conf_champ`` and, when
        ``playoff_seeds`` is set, ``seed``.

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
    st = _add_conf_ranks(st, dg_conf, depth, rng)
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
    return st.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else st


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
        ``game_type``, ``home_team``, ``away_team``, ``result``, ``neutral``
        — the :func:`cfb_standings` / :func:`cfb_simulations` input schema.

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
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
