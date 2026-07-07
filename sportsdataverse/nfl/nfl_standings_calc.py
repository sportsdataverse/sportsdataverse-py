"""NFL standings + playoff seeding -- a reduced, self-contained port of the
win_pct -> head-to-head -> division record -> conference record tiebreaker
ladder.

nflfastR's own ``calculate_standings`` (``calculate_standings.R``) is a thin
dispatch/reshape wrapper: it delegates ALL actual tiebreaker logic to the
external ``nflseedR`` package (``compute_division_ranks`` /
``compute_conference_seeds``) and carries no tiebreaker rules of its own (see
reference Sec 12). ``nflseedR``'s full ladder continues past conference record
through common games / strength of victory / strength of schedule with a
recursive largest-tied-group-first resolution -- reimplementing that in full
is out of scope here (YAGNI per the task brief). This module instead ports a
bounded four-tier ladder -- **win_pct -> head-to-head -> division record ->
conference record** -- gated by ``tiebreaker_depth`` (1: win_pct only, 2: adds
head-to-head + division record, 3 [default]: adds conference record too).
Any tie still unresolved after the configured depth falls back to a
deterministic alphabetical-by-team ordering rather than nflseedR's random
coinflip, so results are reproducible.

The one piece of nflfastR-specific domain knowledge preserved verbatim here is
the 2020 playoff-format cutover: seasons 1999-2019 default to
``playoff_seeds=6``, seasons >= 2020 default to ``playoff_seeds=7``.

.. deprecated::
    :func:`sportsdataverse.nfl.nfl_season_standings` (a faithful polars port of
    the full ``nflseedR`` engine -- common games, strength of victory, strength
    of schedule, draft order) supersedes this reduced ladder. ``calculate_nfl_standings``
    remains for back-compat and emits a ``DeprecationWarning``.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

# One team-season record (as produced by ``.to_dicts()``); keys include
# season/conf/division/team/games/wins/losses/ties/win_pct/div_pct/conf_pct
# plus the later-assigned div_rank/seed.
_Record = dict[str, Any]

# Tiers applied cumulatively above win_pct, gated by tiebreaker_depth.
_TIERS_BY_DEPTH: dict[int, tuple[str, ...]] = {
    1: (),
    2: ("h2h", "div_pct"),
    3: ("h2h", "div_pct", "conf_pct"),
}

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "conf",
    "division",
    "div_rank",
    "seed",
    "team",
    "games",
    "wins",
    "losses",
    "ties",
    "win_pct",
    "div_pct",
    "conf_pct",
)


def _empty_standings(*, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row frame carrying the documented schema."""
    int_cols = {"season", "games", "wins", "losses", "ties", "div_rank", "seed"}
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    for c in _OUTPUT_COLUMNS:
        if c in ("conf", "division", "team"):
            schema[c] = pl.Utf8
        elif c in int_cols:
            schema[c] = pl.Int64
        else:
            schema[c] = pl.Float64
    out = pl.DataFrame(schema=schema)
    if return_as_pandas:
        return out.to_pandas()
    return out


def _double_games(games: pl.DataFrame) -> pl.DataFrame:
    """One row per (season, team, opp, outcome) for each direction of each game.

    ``outcome`` is 1.0 (win) / 0.0 (loss) / 0.5 (tie), keyed off
    ``home_score - away_score``.
    """
    g = games.filter(
        (pl.col("game_type") == "REG") & pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()
    ).with_columns((pl.col("home_score") - pl.col("away_score")).alias("result"))

    home_outcome = pl.when(pl.col("result") > 0).then(1.0).when(pl.col("result") < 0).then(0.0).otherwise(0.5)

    home = g.select(
        "season",
        pl.col("home_team").alias("team"),
        pl.col("away_team").alias("opp"),
        home_outcome.alias("outcome"),
    )
    away = g.select(
        "season",
        pl.col("away_team").alias("team"),
        pl.col("home_team").alias("opp"),
        (1.0 - home_outcome).alias("outcome"),
    )
    return pl.concat([home, away])


def _with_team_meta(doubled: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    """Join conference/division for both ``team`` and ``opp``, add game-type flags."""
    meta = teams.select(
        pl.col("team_abbr").alias("team"),
        pl.col("team_conf").alias("conf"),
        pl.col("team_division").alias("division"),
    )
    out = doubled.join(meta, on="team", how="left")
    opp_meta = meta.rename({"team": "opp", "conf": "opp_conf", "division": "opp_division"})
    out = out.join(opp_meta, on="opp", how="left")
    return out.with_columns(
        (pl.col("division") == pl.col("opp_division")).cast(pl.Int64).alias("div_game"),
        (pl.col("conf") == pl.col("opp_conf")).cast(pl.Int64).alias("conf_game"),
    )


def _team_records(doubled_meta: pl.DataFrame) -> pl.DataFrame:
    """Per-(season, team) games/wins/losses/ties/win_pct/div_pct/conf_pct."""
    return doubled_meta.group_by(["season", "conf", "division", "team"]).agg(
        pl.len().cast(pl.Int64).alias("games"),
        (pl.col("outcome") == 1.0).sum().cast(pl.Int64).alias("wins"),
        (pl.col("outcome") == 0.0).sum().cast(pl.Int64).alias("losses"),
        (pl.col("outcome") == 0.5).sum().cast(pl.Int64).alias("ties"),
        (pl.col("outcome").sum() / pl.len()).alias("win_pct"),
        pl.when((pl.col("div_game") == 1).sum() == 0)
        .then(0.5)
        .otherwise((pl.col("outcome") * pl.col("div_game")).sum() / pl.col("div_game").sum())
        .alias("div_pct"),
        pl.when((pl.col("conf_game") == 1).sum() == 0)
        .then(0.5)
        .otherwise((pl.col("outcome") * pl.col("conf_game")).sum() / pl.col("conf_game").sum())
        .alias("conf_pct"),
    )


def _h2h_lookup(doubled: pl.DataFrame) -> dict[tuple[int, str, str], tuple[int, float]]:
    """``(season, team, opp) -> (games, wins)`` for head-to-head tiebreaks."""
    agg = doubled.group_by(["season", "team", "opp"]).agg(
        pl.len().cast(pl.Int64).alias("h2h_games"),
        pl.col("outcome").sum().alias("h2h_wins"),
    )
    return {(r["season"], r["team"], r["opp"]): (r["h2h_games"], r["h2h_wins"]) for r in agg.to_dicts()}


def _break_ties(
    candidates: list[_Record],
    tiers: tuple[str, ...],
    h2h: dict[tuple[int, str, str], tuple[int, float]],
) -> list[_Record]:
    """Order a tied group best-first, applying ``tiers`` in sequence.

    Any subgroup still tied after every configured tier is ordered
    alphabetically by team (deterministic fallback -- ponytail: real
    nflseedR breaks a residual tie with a random coinflip; this port stays
    deterministic and does not implement common-games / strength-of-victory /
    strength-of-schedule, add if a caller needs the full ladder).
    """
    groups: list[list[_Record]] = [list(candidates)]
    for tier in tiers:
        next_groups: list[list[_Record]] = []
        for grp in groups:
            if len(grp) < 2:
                next_groups.append(grp)
                continue
            if tier == "h2h":
                names = {t["team"] for t in grp}
                scored = []
                for t in grp:
                    h2h_g = h2h_w = 0.0
                    for opp in names:
                        if opp == t["team"]:
                            continue
                        rec = h2h.get((t["season"], t["team"], opp))
                        if rec:
                            h2h_g += rec[0]
                            h2h_w += rec[1]
                    value = 0.5 if h2h_g == 0 else h2h_w / h2h_g
                    scored.append((value, t))
            else:
                scored = [(t[tier], t) for t in grp]
            best_val = max(v for v, _ in scored)
            best = [t for v, t in scored if v == best_val]
            rest = [t for v, t in scored if v != best_val]
            next_groups.append(best)
            if rest:
                next_groups.append(rest)
        groups = next_groups

    ordered: list[_Record] = []
    for grp in groups:
        ordered.extend(sorted(grp, key=lambda t: t["team"]) if len(grp) > 1 else grp)
    return ordered


def _rank_group(
    records: list[_Record],
    tiers: tuple[str, ...],
    h2h: dict[tuple[int, str, str], tuple[int, float]],
    *,
    start: int = 1,
) -> dict[str, int]:
    """Assign 1-based ranks within a single group (division or conference pool)."""
    ranks: dict[str, int] = {}
    remaining = list(records)
    rank = start
    while remaining:
        max_wp = max(r["win_pct"] for r in remaining)
        tied = [r for r in remaining if r["win_pct"] == max_wp]
        ordered = _break_ties(tied, tiers, h2h) if len(tied) > 1 else tied
        for r in ordered:
            ranks[r["team"]] = rank
            rank += 1
        remaining = [r for r in remaining if r["team"] not in ranks]
    return ranks


def _default_playoff_seeds(season: int) -> int:
    """2020 playoff-format cutover: 6 seeds pre-2020, 7 from 2020 on."""
    return 6 if season <= 2019 else 7


@overload
def calculate_nfl_standings(
    games: pl.DataFrame,
    *,
    teams: pl.DataFrame | None = ...,
    tiebreaker_depth: int = ...,
    playoff_seeds: int | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def calculate_nfl_standings(
    games: pl.DataFrame,
    *,
    teams: pl.DataFrame | None = ...,
    tiebreaker_depth: int = ...,
    playoff_seeds: int | None = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def calculate_nfl_standings(
    games: pl.DataFrame,
    *,
    teams: pl.DataFrame | None = None,
    tiebreaker_depth: int = 3,
    playoff_seeds: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Compute NFL division standings + conference playoff seeds.

    A reduced port of the tiebreaker ladder nflfastR delegates to the external
    ``nflseedR`` package (see the module docstring for the exact scope). Games
    are doubled into one row per team per game, regular-season win/loss/tie
    records are computed per team, and ties are broken win_pct -> head-to-head
    -> division record -> conference record, to the depth configured by
    ``tiebreaker_depth``.

    Args:
        games: A ``load_nfl_schedule``-shaped frame: ``game_id``, ``season``,
            ``game_type``, ``week``, ``home_team``, ``away_team``,
            ``home_score``, ``away_score``. Only ``game_type == "REG"`` rows
            with both scores present are used.
        teams: A ``load_nfl_teams``-shaped frame (``team_abbr``, ``team_conf``,
            ``team_division``). When ``None`` (default), calls
            :func:`sportsdataverse.nfl.load_nfl_teams`. Must cover every team
            abbreviation appearing in ``games`` -- a team absent from
            ``teams`` gets null ``conf``/``division`` and is silently pooled
            into the ``(season, None)`` division/conference group rather than
            raising.
        tiebreaker_depth: ``1`` (win_pct only), ``2`` (adds head-to-head +
            division record), or ``3`` (default; adds conference record too).
        playoff_seeds: Number of teams per conference that receive a
            non-null ``seed``. When ``None`` (default), uses the 2020 playoff
            -format cutover: ``6`` for seasons <= 2019, ``7`` for 2020+.
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame with one row per (season, team):
        ``conf``, ``division``, ``div_rank``, ``seed`` (null past
        ``playoff_seeds``), ``team``, ``games``, ``wins``, ``losses``,
        ``ties``, ``win_pct`` (ties count as 0.5 win), ``div_pct``,
        ``conf_pct``. Sorted by ``(season, division, div_rank, seed)``.

    Raises:
        ValueError: If ``tiebreaker_depth`` is not ``1``, ``2``, or ``3``.

    Example:
        Quick start::

            from sportsdataverse.nfl import calculate_nfl_standings, load_nfl_schedule
            games = load_nfl_schedule(seasons=[2023])
            standings = calculate_nfl_standings(games)
            standings.filter(standings["div_rank"] == 1)

        Injected teams frame (offline)::

            standings = calculate_nfl_standings(games, teams=my_teams_df)

        Pipeline next step (one line)::

            standings.sort(["conf", "seed"]).select("team", "seed", "win_pct")

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- ``calculate_standings`` (dispatch-only)
        * `nflseedR <https://github.com/nflverse/nflseedR>`_ -- the real tiebreaker engine
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)

    .. _nflfastR: https://www.nflfastr.com
    .. _nflseedR: https://github.com/nflverse/nflseedR
    .. _nflreadpy: https://github.com/nflverse/nflreadpy

    .. deprecated::
        Prefer :func:`sportsdataverse.nfl.nfl_season_standings`, a faithful
        polars port of the full ``nflseedR`` tiebreaker engine (common games,
        strength of victory, strength of schedule, draft order) that this
        bounded four-tier ladder only approximates. This function remains for
        back-compat and emits a ``DeprecationWarning``.
    """
    warnings.warn(
        "calculate_nfl_standings is a reduced (four-tier) tiebreaker ladder; "
        "prefer nfl_season_standings for the faithful full-nflseedR engine "
        "(SOV/SOS/common-games/draft-order).",
        DeprecationWarning,
        stacklevel=2,
    )
    if tiebreaker_depth not in (1, 2, 3):
        raise ValueError(f"Invalid tiebreaker_depth {tiebreaker_depth!r}; expected 1, 2, or 3.")

    doubled = _double_games(games)
    if doubled.height == 0:
        return _empty_standings(return_as_pandas=return_as_pandas)

    if teams is None:
        from sportsdataverse.nfl.nfl_loaders import load_nfl_teams  # noqa: PLC0415

        teams = load_nfl_teams()

    doubled_meta = _with_team_meta(doubled, teams)
    records = _team_records(doubled_meta)
    h2h = _h2h_lookup(doubled)
    tiers = _TIERS_BY_DEPTH[tiebreaker_depth]

    rows = records.to_dicts()

    # -- Division rank: rank within each (season, division) pool. -----------
    by_division: dict[tuple[int, str], list[_Record]] = {}
    for r in rows:
        by_division.setdefault((r["season"], r["division"]), []).append(r)

    div_ranks: dict[tuple[int, str], int] = {}
    for (season, division), grp in by_division.items():
        ranks = _rank_group(grp, tiers, h2h)
        for team, rank in ranks.items():
            div_ranks[(season, team)] = rank
    for r in rows:
        r["div_rank"] = div_ranks[(r["season"], r["team"])]

    # -- Conference seed: division winners first, then wildcards. -----------
    by_conf: dict[tuple[int, str], list[_Record]] = {}
    for r in rows:
        by_conf.setdefault((r["season"], r["conf"]), []).append(r)

    seeds: dict[tuple[int, str], int | None] = {}
    for (season, conf), grp in by_conf.items():
        n_seeds = playoff_seeds if playoff_seeds is not None else _default_playoff_seeds(season)
        winners = [r for r in grp if r["div_rank"] == 1]
        wildcards = [r for r in grp if r["div_rank"] != 1]
        ranks = _rank_group(winners, tiers, h2h, start=1)
        ranks.update(_rank_group(wildcards, tiers, h2h, start=len(winners) + 1))
        for r in grp:
            rank = ranks[r["team"]]
            seeds[(season, r["team"])] = rank if rank <= n_seeds else None
    for r in rows:
        r["seed"] = seeds[(r["season"], r["team"])]

    out = pl.DataFrame(rows, schema_overrides={"seed": pl.Int64}).select(list(_OUTPUT_COLUMNS))
    out = out.sort(["season", "division", "div_rank", "seed"])

    if return_as_pandas:
        return out.to_pandas()
    return out
