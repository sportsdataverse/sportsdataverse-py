"""College football season simulations (nflseedR-style engine).

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe):
https://nflseedr.com. Mirrors nflseedR's week-loop simulation with a
pluggable ``compute_results`` (``R/simulations.R``,
``nflseedR_compute_results`` in ``R/simulations_utils.R``) with the CFB
adaptations from the shared seedr-port spec.

CFB simplifications (documented deliberately — the R ``cfbseedR`` port uses
the SAME semantics so outputs cross-validate):

* **Sequential loop, no chunking.** nflseedR chunks simulations for
  parallelism; this port stacks all sims in one frame and loops weeks once.
* **Default results model** is nflseedR's ELO variant with its exact
  constants (init ``N(1500, 150)``, +20 non-neutral home bump, x1.2
  postseason multiplier, estimate ``elo_diff / 25``, margin
  ``N(estimate, 13)`` rounded away from zero, K=20 with log-MOV
  multiplier). The NFL rest-day adjustment is dropped (CFB plays weekly).
  Ties remain possible in REG games (engine parity with nflseedR; real CFB
  has had overtime since 1996) and are re-broken by win probability in
  postseason games.
* **Simulated postseason** = conference championship games + the CFP
  bracket from :func:`cfb_playoff_seeds`. CONF_CHAMP rows present in the
  input are simulated as given (their matchups may not be each sim's true
  rank-1/rank-2 pair — realistic projection input); conferences without a
  CONF_CHAMP row get a generated rank-1 vs rank-2 game (rank 1 home,
  neutral flag set).
* **Fixed CFP bracket, no reseeding.** Standard bracket order with the top
  seeds on byes (byes = next power of two minus field size). For the
  12-team CFP: 12@5, 11@6, 10@7 and 9@8 on campus; quarterfinals 1 v 8/9,
  2 v 7/10, 3 v 6/11, 4 v 5/12; semifinals the 1/4 side v the 2/3 side.
  First round at the better seed's home, later rounds neutral.
* **Season rules.** A ``season``-keyed input season picks the tiebreaker
  registry epochs and CFP selection rule (see
  :func:`sportsdataverse.cfb.cfb_playoff_seeds`); ``sim``-keyed input uses
  the current rules.
* **No draft order** (CFB has none).
"""

from __future__ import annotations

from typing import Any, cast
from collections.abc import Callable

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_standings import (
    FrameLike,
    _is_independent,
    _validate_games,
    _validate_teams,
    cfb_standings,
)

__all__ = ["cfb_simulations", "cfb_compute_results"]

_SIM_INCLUDES = ("REG", "CONF", "POST")

ComputeResultsFn = Callable[..., dict[str, pl.DataFrame]]


def _round_out(expr: pl.Expr) -> pl.Expr:
    """Round away from zero (nflseedR ``round_out``)."""
    return pl.when(expr > 0).then(expr.ceil()).when(expr < 0).then(expr.floor()).otherwise(0.0).cast(pl.Float64)


def cfb_compute_results(
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: int,
    *,
    rng: np.random.Generator | None = None,
    elo: dict[str, float] | None = None,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]:
    """Default results generator — nflseedR's dynamic ELO model for CFB.

    Fills ``result`` for week ``week_num`` games that are still unplayed and
    updates each team's ELO rating from that week's results (real results
    included). Constants are nflseedR's ``nflseedR_compute_results`` exactly,
    minus the NFL rest-day adjustment (CFB plays weekly — documented
    simplification).

    Args:
        teams: Per-sim team table (``sim``, ``team``, ``conference``,
            optionally ``elo`` carried over from the previous week).
        games: Per-sim games table (engine schema; see
            :func:`sportsdataverse.cfb.cfb_standings`).
        week_num: The week to fill.
        rng: numpy Generator (seeded by :func:`cfb_simulations`). A fresh
            default generator is created when omitted.
        elo: Optional initial ratings ``{team: elo}`` applied to every sim.
            Teams missing from the dict start at 1500. When neither ``elo``
            nor a ``teams.elo`` column exists, ratings initialize randomly
            at ``N(1500, 150)`` per (sim, team) — nflseedR behavior.
        **kwargs: Ignored (forward-compatibility for custom callers).

    Returns:
        ``{"teams": ..., "games": ...}`` — updated frames, mirroring
        nflseedR's returned list.

    Example:
        Fill week 5 of a stacked sims frame::

            from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
            out = cfb_compute_results(teams, games, 5, rng=rng)
            teams, games = out["teams"], out["games"]

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- ``nflseedR_compute_results``.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if rng is None:
        rng = np.random.default_rng()
    if "elo" not in teams.columns:
        if elo is not None:
            ratings = pl.DataFrame({"team": list(elo.keys()), "elo": [float(v) for v in elo.values()]})
            teams = teams.join(ratings, on="team", how="left").with_columns(pl.col("elo").fill_null(1500.0))
        else:
            teams = teams.with_columns(pl.Series("elo", rng.normal(1500.0, 150.0, teams.height), dtype=pl.Float64))

    g = (
        games.join(
            teams.select("sim", pl.col("team").alias("home_team"), pl.col("elo").alias("_home_elo")),
            on=["sim", "home_team"],
            how="left",
        )
        .join(
            teams.select("sim", pl.col("team").alias("away_team"), pl.col("elo").alias("_away_elo")),
            on=["sim", "away_team"],
            how="left",
        )
        .with_columns(
            (
                pl.col("_home_elo") - pl.col("_away_elo") + pl.when(pl.col("neutral") == 0).then(20.0).otherwise(0.0)
            ).alias("_elo_diff")
        )
        .with_columns(
            pl.when(pl.col("game_type") != "REG")
            .then(pl.col("_elo_diff") * 1.2)
            .otherwise(pl.col("_elo_diff"))
            .alias("_elo_diff")
        )
        .with_columns(
            (1.0 / ((10.0 ** (-pl.col("_elo_diff") / 400.0)) + 1.0)).alias("_wp"),
            (pl.col("_elo_diff") / 25.0).alias("_estimate"),
        )
    )
    n = g.height
    draws = pl.Series("_draw", rng.normal(0.0, 13.0, n), dtype=pl.Float64)
    unifs = pl.Series("_unif", rng.uniform(0.0, 1.0, n), dtype=pl.Float64)
    is_week = pl.col("week") == week_num
    g = (
        g.with_columns(draws, unifs)
        .with_columns(_round_out(pl.col("_estimate") + pl.col("_draw")).alias("_sim_result"))
        .with_columns(
            # postseason games cannot tie — re-break by win probability
            pl.when((pl.col("game_type") != "REG") & (pl.col("_sim_result") == 0))
            .then(pl.when(pl.col("_unif") < pl.col("_wp")).then(1.0).otherwise(-1.0))
            .otherwise(pl.col("_sim_result"))
            .alias("_sim_result")
        )
        .with_columns(
            pl.when(is_week & pl.col("result").is_null())
            .then(pl.col("_sim_result"))
            .otherwise(pl.col("result"))
            .cast(pl.Float64)
            .alias("result")
        )
    )

    # ELO shift from this week's results (K=20, log-MOV multiplier)
    wk = g.filter(is_week & pl.col("result").is_not_null()).with_columns(
        pl.when(pl.col("result") > 0).then(1.0).when(pl.col("result") < 0).then(0.0).otherwise(0.5).alias("_outcome"),
        pl.when(pl.col("result") > 0)
        .then(pl.col("_elo_diff") * 0.001 + 2.2)
        .when(pl.col("result") < 0)
        .then(-pl.col("_elo_diff") * 0.001 + 2.2)
        .otherwise(1.0)
        .alias("_elo_input"),
    )
    wk = wk.with_columns(
        ((pl.max_horizontal(pl.col("result").abs(), pl.lit(1.0)) + 1.0).log() * 2.2 / pl.col("_elo_input")).alias(
            "_elo_mult"
        )
    ).with_columns((20.0 * pl.col("_elo_mult") * (pl.col("_outcome") - pl.col("_wp"))).alias("_shift"))
    shifts = (
        pl.concat(
            [
                wk.select("sim", pl.col("home_team").alias("team"), pl.col("_shift").alias("_delta")),
                wk.select("sim", pl.col("away_team").alias("team"), (-pl.col("_shift")).alias("_delta")),
            ]
        )
        .group_by("sim", "team")
        .agg(pl.col("_delta").sum())
    )
    teams = (
        teams.join(shifts, on=["sim", "team"], how="left")
        .with_columns((pl.col("elo") + pl.col("_delta").fill_null(0.0)).alias("elo"))
        .drop("_delta")
    )

    games_out = g.select(games.columns)
    return {"teams": teams, "games": games_out}


def _apply_compute_results(
    compute_results: ComputeResultsFn,
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: int,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    out = compute_results(teams, games, week_num, rng=rng)
    return out["teams"], out["games"]


def _generate_conf_champ_games(standings: pl.DataFrame, games: pl.DataFrame, week: int) -> pl.DataFrame:
    """Rank-1 vs rank-2 CONF_CHAMP games for conferences lacking one in the input."""
    have = (
        games.filter(pl.col("game_type") == "CONF_CHAMP")
        .join(
            standings.select("sim", pl.col("team").alias("home_team"), "conference"),
            on=["sim", "home_team"],
            how="left",
        )
        .select("sim", "conference")
        .unique()
    )
    top2 = standings.filter((_is_independent() == False) & (pl.col("conf_rank") <= 2)).join(  # noqa: E712
        have, on=["sim", "conference"], how="anti"
    )
    r1 = top2.filter(pl.col("conf_rank") == 1).select("sim", "conference", pl.col("team").alias("home_team"))
    r2 = top2.filter(pl.col("conf_rank") == 2).select("sim", "conference", pl.col("team").alias("away_team"))
    return r1.join(r2, on=["sim", "conference"], how="inner").select(
        "sim",
        pl.lit(week, dtype=pl.Int64).alias("week"),
        pl.lit("CONF_CHAMP").alias("game_type"),
        "home_team",
        "away_team",
        pl.lit(None, dtype=pl.Float64).alias("result"),
        pl.lit(1, dtype=pl.Int64).alias("neutral"),
    )


def _n_byes(n: int) -> int:
    """First-round byes in a fixed bracket of ``n`` seeds (4 for the 12-team CFP)."""
    return (1 << (n - 1).bit_length()) - n if n > 1 else 0


def _bye_cutoffs(standings: pl.DataFrame) -> pl.DataFrame:
    """(sim, _bye_cut): seeds 1.._bye_cut drew a first-round bye in that sim's actual field."""
    n_field = standings.group_by("sim").agg(pl.col("seed").max().fill_null(0).alias("_n"))
    cut = {n: _n_byes(n) for n in n_field["_n"].unique().to_list()}
    return n_field.select("sim", pl.col("_n").replace_strict(cut, return_dtype=pl.Int64).alias("_bye_cut"))


def _bracket_slots(n: int) -> dict[int, int]:
    """seed -> bracket slot of a fixed single-elimination bracket for ``n`` seeds.

    Standard bracket order (1 v P, P/2 v P/2+1, ...; P = next power of two),
    so the top ``P - n`` seeds draw byes. For 12 this is the CFP bracket:
    1 v 8/9 and 4 v 5/12 on one side, 2 v 7/10 and 3 v 6/11 on the other.
    """
    order = [1]
    while len(order) < n:
        m = 2 * len(order) + 1
        order = [s for x in order for s in (x, m - x)]
    return {seed: slot for slot, seed in enumerate(order) if seed <= n}


def _simulate_bracket(
    field: pl.DataFrame,
    teams: pl.DataFrame,
    games: pl.DataFrame,
    compute_results: ComputeResultsFn,
    week_start: int,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Fixed single-elimination CFP bracket (no reseeding), see :func:`_bracket_slots`.

    First round at the better seed's home, later rounds neutral (better seed
    listed home). Returns (champions frame [sim, team], teams, games).
    """
    if field.height == 0:
        return pl.DataFrame(schema={"sim": pl.Int64, "team": pl.Utf8}), teams, games
    n = int(cast(int, field.get_column("seed").max()))
    slots = _bracket_slots(n)
    slot_df = pl.DataFrame(
        {"seed": list(slots), "_slot": list(slots.values())}, schema={"seed": pl.Int64, "_slot": pl.Int64}
    )
    alive = field.select("sim", "team", pl.col("seed").cast(pl.Int64)).join(slot_df, on="seed")
    week = week_start
    for r in range(1, (n - 1).bit_length() + 1):
        # teams sharing slot // 2**r meet this round; a lone team has a bye
        a = (
            alive.with_columns((pl.col("_slot") // (1 << r)).alias("_m"))
            .sort(["sim", "_m", "seed"])
            .with_columns(pl.int_range(pl.len()).over("sim", "_m").alias("_i"))
        )
        hi, lo = a.filter(pl.col("_i") == 0), a.filter(pl.col("_i") == 1)
        byes = hi.join(lo, on=["sim", "_m"], how="anti").select(alive.columns)
        rnd = hi.join(lo, on=["sim", "_m"], suffix="_low").select(
            "sim",
            pl.lit(week, dtype=pl.Int64).alias("week"),
            pl.lit("POST").alias("game_type"),
            pl.col("team").alias("home_team"),
            pl.col("team_low").alias("away_team"),
            pl.lit(None, dtype=pl.Float64).alias("result"),
            pl.lit(0 if r == 1 else 1, dtype=pl.Int64).alias("neutral"),
        )
        games = pl.concat([games, rnd])
        teams, games = _apply_compute_results(compute_results, teams, games, week, rng)
        played = games.filter((pl.col("week") == week) & (pl.col("game_type") == "POST"))
        winners = played.select(
            "sim",
            pl.when(pl.col("result") > 0).then(pl.col("home_team")).otherwise(pl.col("away_team")).alias("team"),
        ).join(alive, on=["sim", "team"], how="inner")
        alive = pl.concat([byes, winners.select(alive.columns)])
        week += 1
    return alive.select("sim", "team"), teams, games


def _game_summary(games: pl.DataFrame) -> pl.DataFrame:
    """Per unique matchup: games played, home win / tie rates and mean margin."""
    return (
        games.filter(pl.col("result").is_not_null())
        .group_by("game_type", "week", "home_team", "away_team")
        .agg(
            pl.len().cast(pl.Int64).alias("games_played"),
            (pl.col("result") > 0).cast(pl.Float64).mean().alias("home_win"),
            (pl.col("result") == 0).cast(pl.Float64).mean().alias("tie"),
            pl.col("result").mean().cast(pl.Float64).alias("result_mean"),
        )
        .sort("game_type", "week", "home_team")
    )


def cfb_simulations(
    games: FrameLike,
    teams: FrameLike,
    compute_results: ComputeResultsFn | None = None,
    *,
    simulations: int = 10000,
    playoff_seeds: int = 12,
    tiebreaker_depth: str = "SOS",
    sim_include: str = "POST",
    rankings: FrameLike | None = None,
    tiebreaker_data: dict[str, FrameLike] | None = None,
    seed: int | None = None,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame | Any]:
    """Simulate college football seasons (nflseedR-style week loop).

    Replicates the input season ``simulations`` times, fills unplayed games
    week by week through the pluggable ``compute_results``, then simulates
    the postseason (conference championships + CFP bracket) and aggregates
    per-team probabilities. See the module docstring for every documented
    CFB simplification.

    Args:
        games: One season of games in the engine schema (``season`` or
            ``sim``, ``week``, ``game_type``, ``home_team``, ``away_team``,
            ``result`` — null = unplayed, ``neutral``). Played results are
            kept as-is. A ``season`` column picks that season's tiebreakers
            and CFP rule; ``sim``-keyed input uses the current ones.
        teams: Team table (``team``, ``conference``, plus the optional
            :func:`sportsdataverse.cfb.cfb_standings` columns).
        compute_results: Results generator with the signature
            ``fn(teams, games, week_num, **kwargs) -> {"teams": ..., "games": ...}``
            filling ``result`` for that week's unplayed games only. Defaults
            to :func:`cfb_compute_results` (dynamic ELO).
        simulations: Number of simulated seasons (sequential, no chunking).
        playoff_seeds: CFP field size passed to :func:`cfb_playoff_seeds`.
        tiebreaker_depth: nflseedR depth ladder (``RANDOM`` < ``PRE-SOV`` <
            ``SOS`` < ``POINTS``) used by every standings computation.
        sim_include: How deep to simulate: ``"REG"`` (regular season only),
            ``"CONF"`` (+ conference championships) or ``"POST"``
            (+ CFP bracket, default).
        rankings: Optional committee rankings (``team``, ``rank``) forwarded
            to :func:`cfb_playoff_seeds`. When None, seeding falls back to
            the per-sim standings ordering (documented in
            :func:`cfb_playoff_seeds`).
        tiebreaker_data: Optional tiebreaker inputs (``"analytics_ratings"``,
            ``"cfp_rankings"``, ``"apr"`` frames) forwarded unchanged to every
            :func:`sportsdataverse.cfb.cfb_standings` call, so the official rungs run.
        seed: Seed for the numpy RNG (deterministic runs).
        return_as_pandas: Return pandas DataFrames instead of polars.

    Returns:
        Dict of frames mirroring the nflseedR summary list:

        * ``"standings"`` — per (sim, team) standings incl. ``conf_rank``,
          ``conf_champ`` and (``sim_include="POST"``) ``seed``.
        * ``"games"`` — all games incl. simulated results and generated
          postseason rows.
        * ``"overall"`` — per-team probabilities (``won_conf``,
          ``made_playoff``, ``first_round_bye``, ``won_cfp``) and mean
          record columns.
        * ``"game_summary"`` — per unique matchup: games played, home win /
          tie rates and mean margin.

    Raises:
        ValueError: On invalid ``sim_include`` / ``tiebreaker_depth``,
            multi-season input, missing columns, or duplicate team rows.

    Example:
        Simulate a season 100 times, deterministically::

            from sportsdataverse.cfb import cfb_simulations
            out = cfb_simulations(games, teams, simulations=100, seed=42,
                                  playoff_seeds=12)
            print(out["overall"].sort("won_cfp", descending=True).head())

        Regular season only::

            out = cfb_simulations(games, teams, simulations=100,
                                  sim_include="REG", seed=1)

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the engine this adapts.
        * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- CFB data in R.
    """
    if sim_include not in _SIM_INCLUDES:
        raise ValueError(f"`sim_include` must be one of {_SIM_INCLUDES}; got {sim_include!r}")
    cr: ComputeResultsFn = compute_results if compute_results is not None else cfb_compute_results
    rng = np.random.default_rng(seed)
    g = _validate_games(games)
    t = _validate_teams(teams)
    if g["sim"].n_unique() != 1:
        raise ValueError("`games` must contain exactly one season")
    # a `season`-keyed input names the season whose rules apply; plain `sim` = current rules
    season = int(g["sim"][0]) if "sim" not in list(games.columns) and "season" in list(games.columns) else None
    st_kwargs: dict[str, Any] = {"tiebreaker_depth": tiebreaker_depth, "tiebreaker_data": tiebreaker_data}
    st_kwargs.update(season=season, rng=rng)

    base = g.drop("sim")
    sims_idx = pl.DataFrame({"sim": pl.int_range(1, simulations + 1, eager=True).cast(pl.Int64)})
    games_all = sims_idx.join(base, how="cross")
    teams_all = sims_idx.join(t, how="cross")

    # Regular season week loop -------------------------------------------------
    reg_weeks = (
        games_all.filter((pl.col("game_type") == "REG") & pl.col("result").is_null())
        .get_column("week")
        .unique()
        .sort()
        .to_list()
    )
    for w in reg_weeks:
        teams_all, games_all = _apply_compute_results(cr, teams_all, games_all, int(w), rng)

    # Conference championships -------------------------------------------------
    if sim_include in ("CONF", "POST"):
        interim = cfb_standings(games_all, t, **st_kwargs)
        assert isinstance(interim, pl.DataFrame)
        max_week = int(games_all.get_column("week").max())
        generated = _generate_conf_champ_games(interim, games_all, max_week + 1)
        if generated.height > 0:
            games_all = pl.concat([games_all, generated])
        cc_weeks = (
            games_all.filter((pl.col("game_type") == "CONF_CHAMP") & pl.col("result").is_null())
            .get_column("week")
            .unique()
            .sort()
            .to_list()
        )
        for w in cc_weeks:
            teams_all, games_all = _apply_compute_results(cr, teams_all, games_all, int(w), rng)

    # Final standings (before CFP games so playoff results don't pollute records)
    standings = cfb_standings(
        games_all,
        t,
        playoff_seeds=playoff_seeds if sim_include == "POST" else None,
        rankings=rankings,
        **st_kwargs,
    )
    assert isinstance(standings, pl.DataFrame)

    # CFP bracket ---------------------------------------------------------------
    champions = pl.DataFrame(schema={"sim": pl.Int64, "team": pl.Utf8})
    if sim_include == "POST":
        field = standings.filter(pl.col("seed").is_not_null())
        week_start = int(games_all.get_column("week").max()) + 1
        champions, teams_all, games_all = _simulate_bracket(field, teams_all, games_all, cr, week_start, rng)

    # Aggregation ---------------------------------------------------------------
    champ_probs = champions.group_by("team").agg((pl.len() / simulations).cast(pl.Float64).alias("won_cfp"))
    agg_cols = [
        pl.col("wins").mean().cast(pl.Float64).alias("wins"),
        pl.col("losses").mean().cast(pl.Float64).alias("losses"),
        pl.col("ties").mean().cast(pl.Float64).alias("ties"),
        pl.col("win_pct").mean().cast(pl.Float64).alias("win_pct"),
        pl.col("conf_champ").cast(pl.Float64).mean().alias("won_conf"),
    ]
    if "seed" in standings.columns:
        agg_cols += [
            pl.col("seed").is_not_null().cast(pl.Float64).mean().alias("made_playoff"),
            (pl.col("seed") <= pl.col("_bye_cut")).fill_null(False).cast(pl.Float64).mean().alias("first_round_bye"),
        ]
    overall = (
        (standings.join(_bye_cutoffs(standings), on="sim") if "seed" in standings.columns else standings)
        .group_by("team", "conference")
        .agg(agg_cols)
        .join(champ_probs, on="team", how="left")
        .with_columns(pl.col("won_cfp").fill_null(0.0))
        .sort("won_cfp", "won_conf", "win_pct", descending=[True, True, True])
    )
    if "made_playoff" not in overall.columns:
        overall = overall.with_columns(
            pl.lit(0.0).alias("made_playoff"),
            pl.lit(0.0).alias("first_round_bye"),
        )
    game_summary = _game_summary(games_all)
    games_out = games_all.sort(["sim", "week", "game_type", "home_team"])

    out: dict[str, pl.DataFrame | Any] = {
        "standings": standings,
        "games": games_out,
        "overall": overall,
        "game_summary": game_summary,
    }
    if return_as_pandas:
        out = {k: v.to_pandas(use_pyarrow_extension_array=True) for k, v in out.items()}
    return out
