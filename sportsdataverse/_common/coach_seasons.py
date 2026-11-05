"""Coach seasons and careers (BB-D8): the shared core behind ``{lg}_coach_seasons`` / ``{lg}_coach_careers``.

**Coach of record, per game.** Every completed game of a team-season is credited to the head
coach on the bench for it, from *stints*: the coaches a source lists for the team-season, each
with its game count and wins when the source has them (ESPN's per-coach records do), or with
none (a staff list). :func:`coach_of_record` places the stints as consecutive runs of the
team's games in date order:

* a stint with a known game count ``n`` and win count ``w`` takes a run of ``n`` games holding
  exactly ``w`` wins. Its ``position`` -- the season's opener (``first``), closer (``last``) or
  unknown (``any`` / null) -- pins where the run may sit;
* the games no counted stint covers form one gap, credited to the count-less stint whose
  ``position`` matches it (``any`` matches every gap). When the source lists count-less stints
  but none fits the gap, that order is rejected; when it lists none, the gap goes to nobody;
* a team-season is credited only when exactly one placement fits. A placement that no order fits
  (the counts sum past the games, no run of ``n`` games holds ``w`` wins, or the runs contradict
  the positions) or that several orders fit differently leaves the whole team-season uncredited,
  with a warning, rather than guessing. So does a team-season whose only placement credits
  nobody (an opener and a closer who differ, neither with counts: the change could have come
  in the season or after it); both counts are logged.

So a coach fired after 24 games is credited those 24 games, his successor the rest, and each
coach-season's W-L, ratings and four factors come from those games only. A game is never split
(the coach of record at tip-off gets all of it), and an acting coach who stood in for a game or
two is not separated out unless the source lists him.

**Results.** W-L, points for and against from the final scores (every completed game; a tied
row, a game never played, is dropped). Ratings and the four factors from the possessions
(BB-D3, :mod:`sportsdataverse._common.team_season_rates`), credited per game the same way:
``off_rtg`` / ``def_rtg`` / ``net_rtg``, ``pace`` and ``efg_pct`` .. ``opp_ft_rate``. The rows
carry the counts the rates come from, so :func:`coach_careers` sums counts and recomputes every
rate (a career is never an average of season rates).

**League context** (BB-D2): ``win_pct`` and the twelve rates get ``<m>_rank`` / ``<m>_pct`` /
``<m>_n``. Coach seasons rank within the season among coaches with at least half the most games
any team played; careers rank among coaches with at least one full season of games (the most
games any team played in a season of the input). Both floor at
:data:`~sportsdataverse._common.league_context.MIN_COHORT_TEAMS`.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import logging
from itertools import permutations

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_ranks, attach_sample_sizes
from sportsdataverse._common.team_season_rates import (
    _LOWER_IS_BETTER,
    _RATED,
    COUNT_ROWS_SCHEMA,
    RATE_ROWS_SCHEMA,
    SIDE_COUNTS,
    rates_from_counts,
    season_counts,
)

__all__ = [
    "COACH_CAREERS_SCHEMA",
    "COACH_GAMES_SCHEMA",
    "COACH_SEASONS_SCHEMA",
    "STINTS_SCHEMA",
    "TEAM_GAMES_SCHEMA",
    "coach_careers",
    "coach_of_record",
    "coach_seasons",
]

logger = logging.getLogger(__name__)

#: Completed games, one row per (game, team), with the date that orders a team's season.
TEAM_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "game_date": pl.Date(),
    "team_id": pl.Int64(),
    "team": pl.Utf8(),
    "opp_team_id": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
}

#: The coaches a source lists for a team-season. ``games`` / ``wins`` are the coach's counts for
#: it (null when the source has none); ``position`` places the stint as the season's opener
#: (``"first"``), its closer (``"last"``) or either (``"any"``; null is ``"any"`` for a counted
#: stint). At most one count-less stint per position: a coach who had the whole season is one
#: ``"any"`` stint.
STINTS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "position": pl.Utf8(),
}

#: One row per credited (game, team).
COACH_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
}

_POSITIONS = ("first", "last", "any")
#: The most stints (plus the gap) one team-season is placed from; more is a source error.
_MAX_ITEMS = 6

_RATES = list(_RATED)
_METRICS = ["win_pct", *_RATES]
_SIZES: dict[str, pl.Expr] = {"win_pct": pl.col("games"), **{m: e for m, (e, _) in _RATED.items()}}

#: The possession counts a row carries (``poss_pts`` / ``opp_poss_pts`` are points on possessions,
#: ``pts`` / ``opp_pts`` the final scores).
_POSS_COUNTS: dict[str, pl.DataType] = {
    "pbp_games": pl.Int64(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "poss_pts": pl.Int64(),
    "opp_poss_pts": pl.Int64(),
    "seconds": pl.Float64(),
    **{c: pl.Float64() for c in SIDE_COUNTS},
}
_RECORD: dict[str, pl.DataType] = {
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "win_pct": pl.Float64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
}
_CONTEXT: dict[str, pl.DataType] = {
    **{m: pl.Float64() for m in _RATES},
    **{f"{m}_rank": pl.Float64() for m in _METRICS},
    **{f"{m}_pct": pl.Float64() for m in _METRICS},
    **{f"{m}_n": pl.Int64() for m in _METRICS},
}

COACH_SEASONS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "team": pl.Utf8(),
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
    "role": pl.Utf8(),
    "team_games": pl.Int64(),
    "first_game_date": pl.Date(),
    "last_game_date": pl.Date(),
    **_RECORD,
    **_POSS_COUNTS,
    **_CONTEXT,
}

COACH_CAREERS_SCHEMA: dict[str, pl.DataType] = {
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
    "role": pl.Utf8(),
    "teams": pl.Utf8(),
    "seasons": pl.Int64(),
    "first_season": pl.Int64(),
    "last_season": pl.Int64(),
    **_RECORD,
    **_POSS_COUNTS,
    **_CONTEXT,
}


def _check(frame: pl.DataFrame, schema: dict[str, pl.DataType], name: str) -> pl.DataFrame:
    missing = sorted(set(schema) - set(frame.columns))
    if missing:
        raise KeyError(f"{name} missing columns: {missing}")
    if "game_id" in schema and frame.schema["game_id"] != pl.Utf8:  # a cast could mis-key ids
        raise TypeError(f"{name}.game_id must be Utf8, got {frame.schema['game_id']}")
    return frame.select(list(schema)).cast(schema)


def _clean_games(team_games: pl.DataFrame) -> pl.DataFrame:
    """``team_games`` with null-id and tied (never played) rows dropped; a repeated row raises."""
    games = _check(team_games, TEAM_GAMES_SCHEMA, "team_games")
    bad = pl.col("team_id").is_null() | pl.col("opp_team_id").is_null() | (pl.col("pts") == pl.col("opp_pts"))
    dropped = games.filter(bad).height
    if dropped:
        logger.warning("dropped %d team-games with no team id or a tied (unplayed) score", dropped)
        games = games.filter(~bad)
    if games.select(pl.struct("game_id", "team_id").is_duplicated().any()).item():
        raise ValueError("team_games: a (game_id, team_id) appears twice")
    return games


def _gap_coach(pos: str, fill: dict[str, int]) -> int | None:
    """The count-less stint that fills a gap at ``pos`` (``all``, ``first``, ``last`` or ``middle``)."""
    if pos in ("first", "last"):
        return fill.get(pos, fill.get("any"))
    return fill.get("any")


def _misplaced(order: tuple, last: int) -> bool:
    """Whether a counted opener does not open, or a counted closer does not close, ``order``."""
    return any(
        (pos == "first" and j != 0) or (pos == "last" and j != last)
        for j, (k, _, _, pos) in enumerate(order)
        if k is not None
    )


def _placements(won: list[bool], known: list[tuple[int, int, int, str]], fill: dict[str, int]) -> set[tuple]:
    """Every distinct per-game stint assignment consistent with the counted stints' runs and positions."""
    gap = len(won) - sum(n for _, n, _, _ in known)
    items: list[tuple[int | None, int, int | None, str]] = list(known)
    if gap > 0:
        items.append((None, gap, None, "any"))
    found: set[tuple] = set()
    last = len(items) - 1
    for order in set(permutations(items)):
        if _misplaced(order, last):
            continue
        out: list[int | None] = []
        for j, (k, n, w, _) in enumerate(order):
            if k is None:
                who = _gap_coach("all" if last == 0 else "first" if j == 0 else "last" if j == last else "middle", fill)
                if who is None and fill:
                    break  # the source names who held the season's ends, and nobody fits this gap
                out += [who] * n
            elif sum(won[len(out) : len(out) + n]) == w:
                out += [k] * n
            else:
                break
        else:
            found.add(tuple(out))
    return found


def _assign(won: list[bool], stints: list[dict]) -> list[int | None] | None:
    """One team-season's per-game stint index, or ``None`` when no single placement fits."""
    known = [(i, s["games"], s["wins"], s["position"] or "any") for i, s in enumerate(stints) if s["games"] is not None]
    fill: dict[str, int] = {}
    for i, s in enumerate(stints):
        if s["games"] is None and s["position"] in _POSITIONS:
            if s["position"] in fill:
                raise ValueError(f"stints: two count-less {s['position']} stints for {s['season']} team {s['team_id']}")
            fill[s["position"]] = i
    too_many = sum(n for _, n, _, _ in known) > len(won) or len(known) + 1 > _MAX_ITEMS
    if too_many or any(w is None for _, _, w, _ in known):
        return None
    found = _placements(won, known, fill)
    return list(found.pop()) if len(found) == 1 else None


def coach_of_record(team_games: pl.DataFrame, stints: pl.DataFrame) -> pl.DataFrame:
    """Credit each completed game to its head coach from the team-season's stints.

    Args:
        team_games: Completed games in :data:`TEAM_GAMES_SCHEMA`.
        stints: The listed coaches in :data:`STINTS_SCHEMA`.

    Returns:
        One row per credited (game, team) in :data:`COACH_GAMES_SCHEMA`, sorted by season, team
        and game. A team-season with no stint, that no single placement fits, or whose placement
        credits nobody (see the module docstring) has no rows; the last two, and the team-seasons
        credited only in part, are logged.

    Raises:
        KeyError: If a frame misses a schema column.
        TypeError: If ``team_games.game_id`` is not text.
        ValueError: If a (game, team) repeats, or a team-season has two count-less stints at one
            position.
    """
    games = _clean_games(team_games).sort("season", "team_id", "game_date", "game_id")
    listed = _check(stints, STINTS_SCHEMA, "stints")
    if listed["coach_id"].null_count():
        logger.warning("dropped %d stints with no coach id", listed["coach_id"].null_count())
        listed = listed.filter(pl.col("coach_id").is_not_null())
    by_team: dict[tuple, list[dict]] = {}
    for s in listed.iter_rows(named=True):
        by_team.setdefault((s["season"], s["team_id"]), []).append(s)
    out: list[dict] = []
    unplaced = nobody = partial = 0
    for (season, team_id), g in games.group_by("season", "team_id", maintain_order=True):
        team_stints = by_team.get((season, team_id))
        if not team_stints:
            continue
        who = _assign((g["pts"] > g["opp_pts"]).to_list(), team_stints)
        if who is None:
            unplaced += 1
            continue
        nobody += all(k is None for k in who)
        partial += any(k is None for k in who) and not all(k is None for k in who)
        for game_id, k in zip(g["game_id"].to_list(), who):
            if k is not None:
                s = team_stints[k]
                out.append(
                    {
                        "season": season,
                        "game_id": game_id,
                        "team_id": team_id,
                        "coach_id": s["coach_id"],
                        "coach": s["coach"],
                    }
                )
    if unplaced or nobody or partial:
        logger.warning(
            "coach_of_record: %d team-seasons uncredited (no single placement fits), %d uncredited (the "
            "placement credits nobody), %d credited in part",
            unplaced,
            nobody,
            partial,
        )
    return (
        pl.DataFrame(out, schema=COACH_GAMES_SCHEMA, orient="row") if out else pl.DataFrame(schema=COACH_GAMES_SCHEMA)
    )


def _keyed(frame: pl.DataFrame, keys: pl.DataFrame) -> pl.DataFrame:
    """Possession / count rows with each side's team id swapped for its coach-stint ``_key``."""
    out = frame
    for side in ("offense", "defense"):
        k = keys.rename({"team_id": f"{side}_team_id", "_key": f"_{side}_key"})
        out = out.join(k, on=["game_id", f"{side}_team_id"], how="left", validate="m:1")
    return out.with_columns(
        pl.col("_offense_key").alias("offense_team_id"), pl.col("_defense_key").alias("defense_team_id")
    ).drop("_offense_key", "_defense_key")


def _poss_counts(
    rows: pl.DataFrame | None, counts: pl.DataFrame | None, keys: pl.DataFrame, group: list[str]
) -> pl.DataFrame:
    """:data:`_POSS_COUNTS` per ``group`` (``_key`` plus whatever ``keys`` carries)."""
    empty = pl.DataFrame(schema={**keys.select(group).schema, **_POSS_COUNTS})
    if rows is None or rows.height == 0:
        return empty
    rows = _check(rows, RATE_ROWS_SCHEMA, "rows")
    counts = None if counts is None else _check(counts, COUNT_ROWS_SCHEMA, "counts")
    ids = keys.select("game_id", "team_id", "_key")
    keyed = _keyed(rows, ids)
    agg = season_counts(keyed, counts=None if counts is None else _keyed(counts, ids)).filter(pl.col("split") == "all")
    names = {"team_id": "_key", "games": "pbp_games", "pts": "poss_pts", "opp_pts": "opp_poss_pts"}
    agg = agg.drop("season", "split").rename(names)
    labels = keys.select(group).unique("_key")
    return labels.join(agg, on="_key", how="inner", validate="1:1").select(*group, *_POSS_COUNTS).cast(_POSS_COUNTS)


def _with_rates(df: pl.DataFrame, *, minutes_per_game: int, by: tuple[str, ...], qualifier: pl.Expr) -> pl.DataFrame:
    """``win_pct``, the twelve rates and their ranks / percentiles / ``_n``."""
    poss = {"poss_pts": "pts", "opp_poss_pts": "opp_pts"}
    final = {"pts": "_final_pts", "opp_pts": "_final_opp_pts"}
    df = df.with_columns((pl.col("wins") / pl.col("games")).alias("win_pct"))
    df = rates_from_counts(df.rename(final).rename(poss), minutes_per_game=minutes_per_game)
    df = df.rename({v: k for k, v in poss.items()}).rename({v: k for k, v in final.items()})
    return df.pipe(
        attach_ranks,
        _METRICS,
        min_cohort=MIN_COHORT_TEAMS,
        by=by,
        lower_is_better=_LOWER_IS_BETTER,
        qualifier=qualifier,
    ).pipe(attach_sample_sizes, _SIZES)


def coach_seasons(
    team_games: pl.DataFrame,
    coach_games: pl.DataFrame,
    rows: pl.DataFrame | None = None,
    *,
    minutes_per_game: int,
    counts: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """One row per (season, team, head coach): W-L, points, ratings and four factors over his games.

    Args:
        team_games: Completed games in :data:`TEAM_GAMES_SCHEMA`.
        coach_games: :func:`coach_of_record` output.
        rows: Possessions in :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`,
            keyed on the same ``game_id`` / ``team_id`` as ``team_games``. ``None`` leaves the
            ratings null.
        minutes_per_game: Regulation minutes, the pace basis: 48 (NBA) or 40 (WNBA, college).
        counts: Game-level four-factor counts for a source whose possessions carry none
            (college), as in :func:`~sportsdataverse._common.team_season_rates.team_season_rates`.

    Returns:
        One row per (season, team_id, coach_id) in :data:`COACH_SEASONS_SCHEMA`, sorted by
        season, team and first game. ``team_games`` is the team's completed games, so a coach
        with ``games < team_games`` shared the season. ``pbp_games`` counts his games with
        published possessions. Empty input returns that schema with zero rows.

    Raises:
        KeyError: If a frame misses a schema column.
        TypeError: If a ``game_id`` is not text.
        ValueError: If a (game, team) repeats in ``team_games`` or ``coach_games``.
    """
    games = _clean_games(team_games)
    credit = _check(coach_games, COACH_GAMES_SCHEMA, "coach_games")
    if credit.select(pl.struct("game_id", "team_id").is_duplicated().any()).item():
        raise ValueError("coach_games: a (game_id, team_id) appears twice")
    if games.height == 0 or credit.height == 0:
        return pl.DataFrame(schema=COACH_SEASONS_SCHEMA)
    team_n = games.group_by("season", "team_id").agg(pl.len().cast(pl.Int64).alias("team_games"))
    played = games.join(credit.drop("season"), on=["game_id", "team_id"], how="inner", validate="1:1")
    won = pl.col("pts") > pl.col("opp_pts")
    key = ["season", "team_id", "coach_id"]
    record = (
        played.group_by(key)
        .agg(
            pl.col("coach").last(),
            pl.col("team").drop_nulls().last(),
            pl.col("game_date").min().alias("first_game_date"),
            pl.col("game_date").max().alias("last_game_date"),
            pl.len().cast(pl.Int64).alias("games"),
            won.sum().cast(pl.Int64).alias("wins"),
            (~won).sum().cast(pl.Int64).alias("losses"),
            pl.col("pts").sum(),
            pl.col("opp_pts").sum(),
        )
        .join(team_n, on=["season", "team_id"], how="left", validate="m:1")
        .with_columns(pl.struct(key).rank("dense").cast(pl.Int64).alias("_key"))
    )
    keys = played.join(record.select(*key, "_key"), on=key, how="inner").select("game_id", "team_id", "_key")
    poss = _poss_counts(rows, counts, keys, ["_key"])
    out = record.join(poss, on="_key", how="left", validate="1:1").with_columns(pl.lit("HC").alias("role"))
    qualifier = pl.col("games") >= 0.5 * pl.col("team_games").max().over("season")
    out = _with_rates(out, minutes_per_game=minutes_per_game, by=("season",), qualifier=qualifier)
    return (
        out.sort("season", "team_id", "first_game_date").select(list(COACH_SEASONS_SCHEMA)).cast(COACH_SEASONS_SCHEMA)
    )


def coach_careers(seasons: pl.DataFrame, *, minutes_per_game: int) -> pl.DataFrame:
    """Sum :func:`coach_seasons` rows per coach and recompute every rate from the summed counts.

    Args:
        seasons: :func:`coach_seasons` output, any number of seasons and teams.
        minutes_per_game: Regulation minutes, the pace basis.

    Returns:
        One row per ``coach_id`` in :data:`COACH_CAREERS_SCHEMA`, most games first. ``teams``
        lists the teams coached, first to last, comma-joined; ``seasons`` counts distinct
        seasons. A career covers only the seasons in ``seasons``. Ranks are among coaches with at
        least the most games any team played in one season of the input. Empty input returns
        that schema with zero rows.

    Raises:
        KeyError: If ``seasons`` misses a :data:`COACH_SEASONS_SCHEMA` column.
    """
    rows = _check(seasons, COACH_SEASONS_SCHEMA, "seasons")
    if rows.height == 0:
        return pl.DataFrame(schema=COACH_CAREERS_SCHEMA)
    full_season = rows["team_games"].max()
    # a count with no possession rows is null, not zero: a career with no pbp keeps null rates
    summed = [c for c in (*_RECORD, *_POSS_COUNTS) if c != "win_pct"]
    sums = [pl.when(pl.col(c).is_not_null().any()).then(pl.col(c).sum()).alias(c) for c in summed]
    out = (
        rows.sort("season", "first_game_date")
        .group_by("coach_id", maintain_order=True)
        .agg(
            pl.col("coach").last(),
            pl.col("team").drop_nulls().unique(maintain_order=True).str.join(", ").alias("teams"),
            pl.col("season").n_unique().cast(pl.Int64).alias("seasons"),
            pl.col("season").min().alias("first_season"),
            pl.col("season").max().alias("last_season"),
            *sums,
        )
        .with_columns(pl.lit("HC").alias("role"), pl.lit(0, dtype=pl.Int64).alias("_all"))
    )
    out = _with_rates(out, minutes_per_game=minutes_per_game, by=("_all",), qualifier=pl.col("games") >= full_season)
    return (
        out.sort(["games", "coach_id"], descending=[True, False])
        .select(list(COACH_CAREERS_SCHEMA))
        .cast(COACH_CAREERS_SCHEMA)
    )
