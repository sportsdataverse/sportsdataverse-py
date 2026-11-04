"""NBA Cup (BB-D30): group standings with the Cup tiebreakers, the knockout sim, advance and title odds.

**The format** (NBA.com, "Emirates NBA Cup 101: Rules, format and how it works", updated
2026-10-28, https://www.nba.com/news/nba-cup-101, and the NBA Cup FAQ,
https://www.nba.com/news/nba-cup-frequently-asked-questions, both read 2026-11-04). The 30
teams are drawn into six groups of five within their conference and play each group opponent
once (two home, two road) on the Cup nights of November. The six group winners and one wild
card per conference, "the team from each conference with the best record in Group Play games
that finished second in its group", go to a single-elimination knockout of eight.

* **Group tiebreakers**, in order: (1) head-to-head record in Group Play; (2) point differential
  in Group Play; (3) total points scored in Group Play; (4) the previous regular season's
  record; (5) a random drawing. Overtime does not count: a team's point differential is 0 in a
  Group Play game that goes to overtime, and its points scored exclude overtime points. The
  wild-card race and the seeding of the group winners use the same order without head-to-head;
  ties within a group are broken first. The official text gives no multi-team step rule; this
  module applies the NBA standings convention (:mod:`sportsdataverse.nba.nba_season_rules`): a
  criterion that splits the tied teams sends each group still tied back to step 1, and a
  head-to-head record counts once every tied team has met every other.
* **Knockout:** in each conference the group winners are seeds 1-3 by Group Play record and the
  wild card is seed 4; seed 1 hosts seed 4 and seed 2 hosts seed 3 in the quarterfinals. The
  semifinals were in Las Vegas (neutral) in 2023-24 through 2025-26; from 2026-27 the higher
  seed hosts (:data:`SEMIFINAL_HOSTED_FROM`). The final is on a neutral court (Hinkle
  Fieldhouse, Indianapolis, on 2026-12-11).
* **What counts toward the 82** (:data:`COUNTS_TOWARD_RECORD`): Group Play, the quarterfinals
  and the semifinals are regular-season games; the final is not. Every team still plays 82:
  the 22 teams out of the knockout each get two regular-season games on the knockout's off
  nights, scheduled by the league after Group Play ("set by Group Play standings"), and the two
  losing quarterfinalists of each conference play each other once.

These rules are the current ones and are applied to every Cup since the first, 2023-24 (season
2024 here). The 2023-24 announcement (the 2022-23 record as the fourth tiebreaker, the
semifinals and final in Las Vegas, the final not counting) matches them.

**Data.** The Cup games and their groups are in stats.nba.com's ``scheduleleaguev2``
(:func:`sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2`): ``game_subtype``
``in-season`` with ``game_sub_label`` ``"East Group A"`` .. ``"West Group C"`` for Group Play,
``in-season-knockout`` with ``"East Quarterfinal"`` / ``"West Semifinal"`` / ``"Championship"``
for the knockout (``"Quarterfinal"`` / ``"Semifinal"`` and team id 0 while the teams are TBD).
The ``nba_stats_schedules`` release does not mark them (``season_type`` ``nba-cup`` is the final
only). Overtime is read from the ``nba_stats_possessions`` release (``period > 4``).

**The sim** (:func:`simulate_cup_draws`) runs inside the season sim
(:func:`sportsdataverse.nba.nba_season_sim.simulate_pro_season` with ``cup``): each simulated
season's own Group Play margins and rating shifts, the tiebreakers, the knockout (actual results
where played), and the regular-season games the knockout leaves to schedule, which the season
sim counts toward each team's 82. :func:`nba_cup_sim` summarizes those Cups. A backtest of a
past season (a complete release) keeps the real replacement games on the schedule, so its season
totals can stray from 82 while the simulated field differs from the real one; the Cup odds do not
depend on them.

**Simplifications** (``ponytail:`` notes in the code): a simulated game has a margin, not a
score, so a simulated tie on point differential is broken by points scored only through the
games already played; the league's formula for the 22 replacement games is not published (the
real ones pair teams no closer in standing than chance: a mean wild-card-rank gap of 3.6 over
the three Cups against 3.7 for a random pairing), so the sim pairs each conference's eleven at
random, one home and one road game each, two of them across conferences as in every Cup.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_season_rules import team_labels

if TYPE_CHECKING:
    import pandas as pd

    from sportsdataverse.nba.nba_season_sim import GameModel

__all__ = ["nba_cup_sim", "nba_cup_standings"]

#: The first Cup season (2023-24), named by the year it ends.
CUP_FIRST_SEASON = 2024
#: Which Cup rounds are regular-season games (count toward the 82).
COUNTS_TOWARD_RECORD: dict[str, bool] = {"group": True, "quarterfinal": True, "semifinal": True, "final": False}
#: The first season whose semifinals are at the higher seed's arena (neutral before).
SEMIFINAL_HOSTED_FROM = 2027
#: Group tiebreakers after the Group Play record, in order; a random drawing follows.
GROUP_TIEBREAKERS: tuple[str, ...] = ("head_to_head", "point_diff", "points", "prior_record")
#: The wild-card race (the second-place teams) and group-winner seeding: the group order
#: without head-to-head.
WILD_CARD_TIEBREAKERS: tuple[str, ...] = ("point_diff", "points", "prior_record")

#: :func:`cup_games` output, one row per Cup game.
CUP_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "game_date": pl.Date(),
    "cup_round": pl.Utf8(),
    "cup_group": pl.Utf8(),
    "home_team_id": pl.Int64(),
    "away_team_id": pl.Int64(),
    "home_pts": pl.Int64(),
    "away_pts": pl.Int64(),
    "neutral": pl.Boolean(),
}

#: :func:`cup_standings` / :func:`nba_cup_standings` output, one row per team.
CUP_STANDINGS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "conference": pl.Utf8(),
    "cup_group": pl.Utf8(),
    "team_id": pl.Int64(),
    "group_rank": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "games_remaining": pl.Int64(),
    "point_diff": pl.Int64(),
    "points": pl.Int64(),
    "prior_win_pct": pl.Float64(),
    "tiebreak": pl.Utf8(),
    "conference_rank": pl.Int64(),
    "leads_group": pl.Boolean(),
    "holds_wild_card": pl.Boolean(),
    "knockout_seed": pl.Int64(),
}

_ODDS = ("p_win_group", "p_wild_card", "p_quarterfinal", "p_semifinal", "p_final", "p_champion")

#: :func:`simulate_cup` / :func:`nba_cup_sim` output, one row per team.
CUP_SIM_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "conference": pl.Utf8(),
    "cup_group": pl.Utf8(),
    "team_id": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "point_diff": pl.Int64(),
    "adj_net_rtg": pl.Float64(),
    **{c: pl.Float64() for c in _ODDS},
    **{f"{c}_se": pl.Float64() for c in _ODDS},
    "mean_group_rank": pl.Float64(),
    "n_sims": pl.Int64(),
}

_ROUND = (
    pl.when(pl.col("game_subtype") == "in-season")
    .then(pl.lit("group"))
    .when(pl.col("game_sub_label").str.contains("Quarterfinal"))
    .then(pl.lit("quarterfinal"))
    .when(pl.col("game_sub_label").str.contains("Semifinal"))
    .then(pl.lit("semifinal"))
    .when(pl.col("game_sub_label") == "Championship")
    .then(pl.lit("final"))
)


def cup_games(schedule: pl.DataFrame, *, as_of_date: dt.date | None = None) -> pl.DataFrame:
    """The Cup games of a ``scheduleleaguev2`` frame, scores blanked on and after ``as_of_date``.

    Args:
        schedule: :func:`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` rows (any
            superset of ``game_id``, ``game_date``, ``game_subtype``, ``game_sub_label``,
            ``game_status``, ``is_neutral``, ``home_team_id``, ``away_team_id``,
            ``home_team_score``, ``away_team_score``).
        as_of_date: Games on or after it are unplayed. None: every final game is played.

    Returns:
        One row per Cup game in :data:`CUP_GAMES_SCHEMA`: ``season`` (the end year, from the
        game id), ``cup_round`` (group / quarterfinal / semifinal / final), ``cup_group``
        (Group Play only), team ids (null while TBD) and scores (null unless final).
    """
    date = pl.col("game_date").cast(pl.Utf8).str.slice(0, 10)
    out = (
        schedule.filter(pl.col("game_subtype").is_in(["in-season", "in-season-knockout"]))
        .with_columns(
            game_date=pl.coalesce(
                date.str.to_date("%m/%d/%Y", strict=False), date.str.to_date("%Y-%m-%d", strict=False)
            )
        )
        .select(
            (2001 + pl.col("game_id").cast(pl.Utf8).str.slice(3, 2).cast(pl.Int64)).alias("season"),
            pl.col("game_id").cast(pl.Utf8),
            "game_date",
            _ROUND.alias("cup_round"),
            pl.when(pl.col("game_subtype") == "in-season").then(pl.col("game_sub_label")).alias("cup_group"),
            *(pl.col(f"{s}_team_id").cast(pl.Int64).replace(0, None).alias(f"{s}_team_id") for s in ("home", "away")),
            *(
                pl.when(pl.col("game_status") == 3).then(pl.col(f"{s}_team_score")).cast(pl.Int64).alias(f"{s}_pts")
                for s in ("home", "away")
            ),
            pl.col("is_neutral").cast(pl.Boolean).alias("neutral"),
        )
    )
    if as_of_date is not None:
        out = out.with_columns(
            pl.when(pl.col("game_date") >= as_of_date).then(None).otherwise(pl.col(c)).alias(c)
            for c in ("home_pts", "away_pts")
        )
    return out.cast(CUP_GAMES_SCHEMA).sort("game_date", "game_id")


def overtime_points(possessions: pl.DataFrame) -> pl.DataFrame:
    """``game_id``, ``team_id``, ``ot_points``: each team's overtime points (``period > 4``) per game."""
    return (
        possessions.filter(pl.col("period") > 4)
        .group_by(pl.col("game_id").cast(pl.Utf8), pl.col("offense_team_id").cast(pl.Int64).alias("team_id"))
        .agg(pl.col("points").sum().cast(pl.Int64).alias("ot_points"))
    )


@dataclass(frozen=True)
class CupInputs:
    """One season's Cup, team-indexed for the sim (:func:`prepare_cup`).

    Attributes:
        season: The season (end year).
        teams: The 30 team ids, in index order.
        group: ``(T,)`` group labels; ``conf``: ``(T,)`` conferences.
        prior: ``(T,)`` previous regular-season win percentage (0 for a team with none).
        played: Group Play games played: ``(home, away, home_won, home_diff, home_pts,
            away_pts)`` arrays, the overtime rule applied (diff 0, regulation points).
        future: ``(home, away)`` index arrays of the Group Play games still to play.
        knockout: Played knockout games: ``{frozenset(team indices): (home, away, home_margin)}``.
        team_pts: Mean regulation points per team-game so far (the base of a simulated game).
        future_ids: The game ids of ``future``, in order.
        knockout_ids: The game ids of every knockout game, the final included.
    """

    season: int
    teams: list[int]
    group: np.ndarray
    conf: np.ndarray
    prior: np.ndarray
    played: tuple[np.ndarray, ...]
    future: tuple[np.ndarray, np.ndarray]
    knockout: dict[frozenset[int], tuple[int, int, float]]
    team_pts: float
    future_ids: tuple[str, ...] = ()
    knockout_ids: tuple[str, ...] = ()


def prepare_cup(
    games: pl.DataFrame, *, ot_points: pl.DataFrame | None = None, prior_records: pl.DataFrame | None = None
) -> CupInputs:
    """Index one season's Cup games (:func:`cup_games`) for the standings and the sim.

    Args:
        games: One season's :func:`cup_games` rows.
        ot_points: :func:`overtime_points` of the Group Play games (None: no game went to
            overtime).
        prior_records: ``team_id``, ``wins``, ``losses`` of the previous regular season.

    Returns:
        A :class:`CupInputs`.

    Raises:
        ValueError: If ``games`` holds more than one season or a group is not five teams.
    """
    if games.get_column("season").n_unique() != 1:
        raise ValueError("games must hold exactly one season")
    group_games = games.filter(pl.col("cup_round") == "group")
    sides = pl.concat(
        [group_games.select(pl.col(f"{s}_team_id").alias("team_id"), "cup_group") for s in ("home", "away")]
    ).unique()
    if sides.group_by("cup_group").len().get_column("len").to_list() != [5] * 6 or sides.height != 30:
        raise ValueError("expected six groups of five teams in the Group Play games")
    teams = sorted(sides.get_column("team_id").to_list())
    index = {t: i for i, t in enumerate(teams)}
    gmap = dict(sides.iter_rows())
    conf, _ = team_labels(teams, "00")
    prior = np.zeros(len(teams))
    if prior_records is not None:
        assert prior_records.schema["team_id"] == pl.Int64, "prior_records team_id must be Int64"
        for t, w, lo in prior_records.select("team_id", "wins", "losses").iter_rows():
            if t in index and w + lo:
                prior[index[t]] = w / (w + lo)
    done = pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    played = group_games.filter(done)
    ot = (
        ot_points
        if ot_points is not None
        else pl.DataFrame(schema={"game_id": pl.Utf8, "team_id": pl.Int64, "ot_points": pl.Int64})
    )
    assert ot.schema["game_id"] == played.schema["game_id"], "game_id dtype differs between overtime points and games"
    for side in ("home", "away"):
        assert ot.schema["team_id"] == played.schema[f"{side}_team_id"], "team_id dtype differs"
        played = played.join(
            ot.rename({"team_id": f"{side}_team_id", "ot_points": f"{side}_ot"}),
            on=["game_id", f"{side}_team_id"],
            how="left",
            validate="1:1",
        )
    is_ot = pl.col("game_id").is_in(ot.get_column("game_id").implode())
    # ponytail: regulation ends tied, so both teams' regulation points are equal; the possessions
    # undercount a point now and then, so take the smaller of the two (matched stats.nba.com's
    # Cup standings on every overtime game of 2024-25 and 2025-26)
    reg = pl.min_horizontal(
        pl.col("home_pts") - pl.col("home_ot").fill_null(0), pl.col("away_pts") - pl.col("away_ot").fill_null(0)
    )
    played = played.with_columns(
        pl.when(is_ot).then(0).otherwise(pl.col("home_pts") - pl.col("away_pts")).alias("diff"),
        pl.when(is_ot).then(reg).otherwise(pl.col("home_pts")).alias("hp"),
        pl.when(is_ot).then(reg).otherwise(pl.col("away_pts")).alias("ap"),
    )

    def ix(frame: pl.DataFrame, col: str) -> np.ndarray:
        return np.array([index[t] for t in frame.get_column(col).to_list()], dtype=np.int64)

    future = group_games.filter(~done)
    knockout: dict[frozenset[int], tuple[int, int, float]] = {}
    for h, a, hp, ap in (
        games.filter(pl.col("cup_round") != "group", done)
        .select("home_team_id", "away_team_id", "home_pts", "away_pts")
        .iter_rows()
    ):
        knockout[frozenset((index[h], index[a]))] = (index[h], index[a], float(hp - ap))
    n_pts = 2 * played.height
    return CupInputs(
        season=int(games.get_column("season")[0]),
        teams=teams,
        group=np.array([gmap[t] for t in teams], dtype=object),
        conf=conf,
        prior=prior,
        played=(
            ix(played, "home_team_id"),
            ix(played, "away_team_id"),
            (played.get_column("home_pts") > played.get_column("away_pts")).to_numpy(),
            played.get_column("diff").to_numpy().astype(float),
            played.get_column("hp").to_numpy().astype(float),
            played.get_column("ap").to_numpy().astype(float),
        ),
        future=(ix(future, "home_team_id"), ix(future, "away_team_id")),
        knockout=knockout,
        future_ids=tuple(future.get_column("game_id").to_list()),
        knockout_ids=tuple(games.filter(pl.col("cup_round") != "group").get_column("game_id").to_list()),
        team_pts=float((played["hp"].sum() + played["ap"].sum()) / n_pts) if n_pts else 0.0,
    )


Values = Callable[[str, list[int]], "np.ndarray | None"]


def _split(
    block: list[int], values: Values, criteria: tuple[str, ...], rng: np.random.Generator
) -> list[tuple[int, str]]:
    """Break a tie: ``block`` best first, each team with the criterion that placed it."""
    for c in criteria:
        v = values(c, block)
        if v is None or np.all(v == v[0]):
            continue
        out: list[tuple[int, str]] = []
        for level in np.unique(v)[::-1]:
            sub = [t for t, x in zip(block, v, strict=True) if x == level]
            # a criterion that splits the tie sends each group still tied back to step 1
            out += _split(sub, values, criteria, rng) if len(sub) > 1 else [(sub[0], c)]
        return out
    return [(block[i], "random") for i in rng.permutation(len(block))]


def _rank(
    members: list[int], pct: np.ndarray, values: Values, criteria: tuple[str, ...], rng: np.random.Generator
) -> list[tuple[int, str | None]]:
    """``members`` best first by winning percentage, ties broken by ``criteria``, with the deciding criterion."""
    out: list[tuple[int, str | None]] = []
    for level in np.unique(pct[members])[::-1]:
        block = [t for t in members if pct[t] == level]
        out += [(block[0], None)] if len(block) == 1 else _split(block, values, criteria, rng)
    return out


@dataclass(frozen=True)
class _Table:
    """One (simulated or actual) Group Play table, team-indexed."""

    wins: np.ndarray
    games: np.ndarray
    diff: np.ndarray
    pts: np.ndarray
    h2h: np.ndarray  # (T, T) wins of row over column
    met: np.ndarray  # (T, T) games between the pair


def _order_cup(
    cup: CupInputs, tab: _Table, rng: np.random.Generator
) -> tuple[np.ndarray, list[str | None], np.ndarray, np.ndarray]:
    """``(group_rank, tiebreak, conference_rank, knockout_seed)`` of one table (seed 0: out)."""
    n = len(cup.teams)
    pct = np.divide(tab.wins, tab.games, out=np.zeros(n), where=tab.games > 0)
    group_rank: np.ndarray = np.zeros(n, dtype=np.int64)
    stat = {"point_diff": tab.diff, "points": tab.pts, "prior_record": cup.prior}

    def values(c: str, block: list[int]) -> np.ndarray | None:
        b = np.array(block)
        if c != "head_to_head":
            return stat[c][b]
        met = tab.met[np.ix_(b, b)]
        if np.any(met + np.eye(b.size) == 0):  # head-to-head counts once every tied team has met
            return None
        return tab.h2h[np.ix_(b, b)].sum(1) / met.sum(1)

    tiebreak: list[str | None] = [None] * n
    for g in np.unique(cup.group):
        for r, (t, why) in enumerate(_rank(list(np.flatnonzero(cup.group == g)), pct, values, GROUP_TIEBREAKERS, rng)):
            group_rank[t], tiebreak[t] = r + 1, why
    conf_rank: np.ndarray = np.zeros(n, dtype=np.int64)
    seed: np.ndarray = np.zeros(n, dtype=np.int64)
    for c in np.unique(cup.conf):
        in_c = cup.conf == c
        leaders = _rank(list(np.flatnonzero(in_c & (group_rank == 1))), pct, values, WILD_CARD_TIEBREAKERS, rng)
        # conference rank: the group winners, the wild-card race (the seconds), then the rest,
        # as stats.nba.com's Cup standings order them (istWildcardRank)
        tiers = [
            _rank(list(np.flatnonzero(in_c & sel)), pct, values, WILD_CARD_TIEBREAKERS, rng)
            for sel in (group_rank == 2, group_rank > 2)
        ]
        for r, (t, _) in enumerate(leaders + tiers[0] + tiers[1]):
            conf_rank[t] = r + 1
        for s, (t, _) in enumerate(leaders):
            seed[t] = s + 1
        seed[tiers[0][0][0]] = len(leaders) + 1  # the wild card: the best second-place team
    return group_rank, tiebreak, conf_rank, seed


def _table(cup: CupInputs, extra: tuple[np.ndarray, ...] | None = None) -> _Table:
    """The Group Play table of the played games, plus simulated ``(home, away, home_won, diff, hp, ap)``."""
    n = len(cup.teams)
    parts = [cup.played] if extra is None else [cup.played, extra]
    h, a, won, diff, hp, ap = (np.concatenate([p[k] for p in parts]) for k in range(6))
    wins, games, d, pts = np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n)
    h2h, met = np.zeros((n, n)), np.zeros((n, n))
    np.add.at(wins, h, won)
    np.add.at(wins, a, ~won)
    np.add.at(games, h, 1)
    np.add.at(games, a, 1)
    np.add.at(d, h, diff)
    np.add.at(d, a, -diff)
    np.add.at(pts, h, hp)
    np.add.at(pts, a, ap)
    np.add.at(h2h, (h, a), won)
    np.add.at(h2h, (a, h), ~won)
    np.add.at(met, (h, a), 1)
    np.add.at(met, (a, h), 1)
    return _Table(wins, games, d, pts, h2h, met)


def cup_standings(cup: CupInputs, *, as_of_date: dt.date | None = None, seed: int = 20261103) -> pl.DataFrame:
    """Group Play standings with the Cup tiebreakers from the games played (:func:`prepare_cup`).

    Args:
        cup: The season's :class:`CupInputs`.
        as_of_date: Carried into the output only (blank the scores with :func:`cup_games`).
        seed: Seeds the random drawing, the last tiebreaker.

    Returns:
        One row per team in :data:`CUP_STANDINGS_SCHEMA`, sorted by conference, group and rank.
    """
    tab = _table(cup)
    rank, why, conf_rank, ko = _order_cup(cup, tab, np.random.default_rng(seed))
    n_left = np.bincount(np.concatenate(cup.future), minlength=len(cup.teams))
    wild: np.ndarray = np.zeros(len(cup.teams), dtype=bool)
    wild[(ko == 4)] = True
    out = {
        "season": [cup.season] * len(cup.teams),
        "as_of_date": [as_of_date] * len(cup.teams),
        "conference": cup.conf.tolist(),
        "cup_group": cup.group.tolist(),
        "team_id": cup.teams,
        "group_rank": rank.tolist(),
        "wins": tab.wins.astype(int).tolist(),
        "losses": (tab.games - tab.wins).astype(int).tolist(),
        "games_remaining": n_left.tolist(),
        "point_diff": tab.diff.astype(int).tolist(),
        "points": tab.pts.astype(int).tolist(),
        "prior_win_pct": cup.prior.tolist(),
        "tiebreak": why,
        "conference_rank": conf_rank.tolist(),
        "leads_group": (rank == 1).tolist(),
        "holds_wild_card": wild.tolist(),
        "knockout_seed": [int(s) or None for s in ko],
    }
    return pl.DataFrame(out, schema=CUP_STANDINGS_SCHEMA).sort("conference", "cup_group", "group_rank")


@dataclass(frozen=True)
class CupDraws:
    """The simulated Cups (:func:`simulate_cup_draws`), ``(n_sims, T)`` unless noted.

    Attributes:
        group_rank: Group Play finish.
        seed: Knockout seed 1-4 in the conference, 0 out of the knockout.
        semifinal / final / champion: Reached the semifinal / final, won the Cup.
        home / away / margin: ``(n_sims, 30)`` the 30 regular-season games the knockout adds
            (4 quarterfinals, 2 semifinals, 2 games between quarterfinal losers, 22 games for
            the teams out of the knockout), team indices and home margins.
        kind: ``(30,)`` which of the four each column is (:data:`COUNTS_TOWARD_RECORD` keeps
            the final out).
    """

    group_rank: np.ndarray
    seed: np.ndarray
    semifinal: np.ndarray
    final: np.ndarray
    champion: np.ndarray
    home: np.ndarray
    away: np.ndarray
    margin: np.ndarray
    kind: tuple[str, ...]


def simulate_cup_draws(
    cup: CupInputs,
    model: GameModel,
    net: np.ndarray,
    noise: np.ndarray,
    group_margins: np.ndarray,
    rng: np.random.Generator,
) -> CupDraws:
    """The Cup in every simulated season, from the season sim's draws.

    Args:
        cup: :func:`prepare_cup` inputs; team indices are ``cup.teams``.
        model: The season sim's game model (:func:`~sportsdataverse.nba.nba_season_sim.as_of_game_model`).
        net: ``(n_sims, T)`` net ratings with each season's rating shift.
        noise: ``(T, T)`` per-game noise sd of a pair (the season sim's ``_noise_sd``).
        group_margins: ``(n_sims, len(cup.future[0]))`` simulated home margins of the remaining
            Group Play games (the same draws as the season's).
        rng: The season sim's generator.

    Returns:
        A :class:`CupDraws`.
    """
    n_sims, n_t = net.shape
    fh, fa = cup.future
    shape = (n_sims, n_t)
    group_rank, seed = np.zeros(shape, dtype=np.int64), np.zeros(shape, dtype=np.int64)
    r = np.rint(group_margins)  # ponytail: a simulated tie after regulation (r == 0) is overtime: diff 0
    for s in range(n_sims):
        extra = (fh, fa, group_margins[s] > 0, r[s], cup.team_pts + r[s] / 2, cup.team_pts - r[s] / 2)
        group_rank[s], _, _, seed[s] = _order_cup(cup, _table(cup, extra), rng)
    rows = np.arange(n_sims)

    def play(h: np.ndarray, a: np.ndarray, neutral: bool, knockout: bool = True) -> np.ndarray:
        """Home margins of one game per season; a knockout game already played keeps its result."""
        m = (net[rows, h] - net[rows, a] + (0.0 if neutral else 2.0 * model.hfa)) * model.poss / 100.0
        m = m + noise[h, a] * rng.standard_normal(n_sims)
        for pair, (kh, _, km) in cup.knockout.items() if knockout else ():
            x, y = tuple(pair)
            hit = ((h == x) & (a == y)) | ((h == y) & (a == x))
            m[hit] = np.where(h[hit] == kh, km, -km)
        return m

    def by_seed(c: str, k: int) -> np.ndarray:
        return np.argmax((seed == k) & (cup.conf == c)[None, :], axis=1)

    semi, fin, champ = np.zeros(shape, dtype=bool), np.zeros(shape, dtype=bool), np.zeros(shape, dtype=bool)
    games: list[tuple[np.ndarray, np.ndarray, np.ndarray, str]] = []
    finalists = []
    hosted = cup.season >= SEMIFINAL_HOSTED_FROM
    for c in ("East", "West"):
        s1, s2, s3, s4 = (by_seed(c, k) for k in (1, 2, 3, 4))
        winners, losers = [], []
        for h, a in ((s1, s4), (s2, s3)):
            m = play(h, a, neutral=False)
            games.append((h, a, m, "quarterfinal"))
            winners.append(np.where(m > 0, h, a))
            losers.append((np.where(m > 0, a, h), m > 0))  # (loser, the loser was the road team)
        semi[rows, winners[0]] = semi[rows, winners[1]] = True
        w1, w2 = winners  # w1 is seed 1 or 4, w2 seed 2 or 3: the higher seed hosts from 2026-27
        top = seed[rows, w1] < seed[rows, w2]
        h, a = np.where(top, w1, w2), np.where(top, w2, w1)
        m = play(h, a, neutral=not hosted)
        games.append((h, a, m, "semifinal"))
        finalists.append(np.where(m > 0, h, a))
        # the losing quarterfinalists meet: a road loser hosts a home loser, else the higher seed
        (l1, road1), (l2, road2) = losers
        first = (road1 & ~road2) | ((road1 == road2) & (seed[rows, l1] < seed[rows, l2]))
        h, a = np.where(first, l1, l2), np.where(first, l2, l1)
        games.append((h, a, play(h, a, neutral=False, knockout=False), "quarterfinal_loser"))
    fe, fw = finalists
    fin[rows, fe] = fin[rows, fw] = True
    m = play(fe, fw, neutral=True)
    champ[rows, np.where(m > 0, fe, fw)] = True
    games.append((fe, fw, m, "final"))
    games += _replacement_games(cup, seed == 0, play, rng)
    # the regular-season games only: the final does not count toward the 82
    kinds = ("quarterfinal", "semifinal", "quarterfinal_loser", "replacement", "final")
    order = sorted(
        (i for i, g in enumerate(games) if COUNTS_TOWARD_RECORD.get(g[3], True)), key=lambda i: kinds.index(games[i][3])
    )
    home, away, margin = (np.stack([games[i][k] for i in order], axis=1) for k in range(3))
    return CupDraws(group_rank, seed, semi, fin, champ, home, away, margin, tuple(games[i][3] for i in order))


def _replacement_games(
    cup: CupInputs,
    out: np.ndarray,
    play: Callable[[np.ndarray, np.ndarray, bool, bool], np.ndarray],
    rng: np.random.Generator,
) -> list[tuple[np.ndarray, np.ndarray, np.ndarray, str]]:
    """The 22 games of the teams out of the knockout: per conference a random path of the 11.

    Each of the path's ten games is hosted by the earlier team, so every team has one home and
    one road game except the two ends, which play across conferences (each Cup had two such
    games): the east path's first team plays at the west path's last, the west's first at the
    east's last.
    """
    paths = {}
    for c in ("East", "West"):
        idx = np.flatnonzero(cup.conf == c)
        keys = rng.random((out.shape[0], idx.size)) + ~out[:, idx]  # out-of-knockout teams sort first, randomly
        paths[c] = idx[np.argsort(keys, axis=1)[:, : int(out[0, idx].sum())]]
    games = []
    for p in paths.values():
        for i in range(p.shape[1] - 1):
            games.append((p[:, i], p[:, i + 1], play(p[:, i], p[:, i + 1], False, False), "replacement"))
    e, w = paths["East"], paths["West"]
    for h, a in ((w[:, -1], e[:, 0]), (e[:, -1], w[:, 0])):
        games.append((h, a, play(h, a, False, False), "replacement"))
    return games


def _summary(cup: CupInputs, draws: CupDraws, tab: _Table) -> dict[str, list]:
    n_sims = draws.seed.shape[0]
    p = {
        "p_win_group": draws.group_rank == 1,
        "p_wild_card": draws.seed == 4,
        "p_quarterfinal": draws.seed > 0,
        "p_semifinal": draws.semifinal,
        "p_final": draws.final,
        "p_champion": draws.champion,
    }
    means = {k: v.mean(0) for k, v in p.items()}
    return {
        "wins": tab.wins.astype(int).tolist(),
        "losses": (tab.games - tab.wins).astype(int).tolist(),
        "point_diff": tab.diff.astype(int).tolist(),
        **{k: v.tolist() for k, v in means.items()},
        **{f"{k}_se": np.sqrt(v * (1 - v) / n_sims).tolist() for k, v in means.items()},
        "mean_group_rank": draws.group_rank.mean(0).tolist(),
        "n_sims": [n_sims] * len(cup.teams),
    }


def simulate_cup(
    games: pl.DataFrame,
    cup: CupInputs,
    model: GameModel,
    *,
    n_sims: int = 10_000,
    seed: int = 20261103,
    rating_uncertainty: bool = True,
    as_of_date: dt.date | None = None,
) -> pl.DataFrame:
    """Monte Carlo of the rest of one Cup inside the season sim: Group Play, tiebreakers, knockout.

    Runs :func:`~sportsdataverse.nba.nba_season_sim.simulate_pro_season`'s engine with the Cup
    (the same draws: one rating shift per team per simulated season, each game's noise, the Group
    Play margins of the season itself) and summarizes its Cups.

    Args:
        games: The season's regular-season games (``season``, ``game_id``, ``home_team_id``,
            ``away_team_id``, ``home_pts`` / ``away_pts``, null for a game to simulate), the Cup
            Group Play games among them with the same scores as ``cup``.
        cup: :func:`prepare_cup` inputs.
        model: The season sim's game model.
        n_sims: Simulated seasons.
        seed: Seeds the generator; the same inputs and seed give the same output.
        rating_uncertainty: Draw one rating shift per team per simulated season.
        as_of_date: Carried into the output only.

    Returns:
        One row per team in :data:`CUP_SIM_SCHEMA`, sorted by conference, group and mean rank.

    Raises:
        ValueError: If ``n_sims < 1`` or a Cup game still to play is not among ``games``' unplayed
            games.
        TypeError: If a team-id dtype differs from the model's ``team_id``.
    """
    from sportsdataverse.nba.nba_season_sim import NBA_SEASON_GAMES, _simulate_season

    season, draws = _simulate_season(
        games,
        model,
        n_sims=n_sims,
        seed=seed,
        rating_uncertainty=rating_uncertainty,
        as_of_date=as_of_date,
        season_games=NBA_SEASON_GAMES,
        cup=cup,
    )
    assert draws is not None
    net = dict(season.select("team_id", "adj_net_rtg").iter_rows())
    out = {
        "season": [cup.season] * len(cup.teams),
        "as_of_date": [as_of_date] * len(cup.teams),
        "conference": cup.conf.tolist(),
        "cup_group": cup.group.tolist(),
        "team_id": cup.teams,
        "adj_net_rtg": [net[t] for t in cup.teams],
        **_summary(cup, draws, _table(cup)),
    }
    return pl.DataFrame(out, schema=CUP_SIM_SCHEMA).sort("conference", "cup_group", "mean_group_rank")


def _season_label(season: int) -> str:
    return f"{season - 1}-{season % 100:02d}"


def cup_inputs(
    cup_schedule: pl.DataFrame,
    *,
    games: pl.DataFrame,
    possessions: pl.DataFrame,
    last_games: pl.DataFrame | None,
    as_of_date: dt.date | None,
) -> CupInputs:
    """:class:`CupInputs` from stats.nba.com's schedule and the season's release frames.

    Args:
        cup_schedule: The season's :func:`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` rows.
        games: The release's regular-season games, scores blanked as of the date. A Cup game it
            holds takes its score from here, so the Cup and the season agree on what is played.
        possessions: The season's possessions release (overtime is ``period > 4``).
        last_games: The previous regular season's games (the fourth tiebreaker); None: none.
        as_of_date: Games on or after it are unplayed.

    Returns:
        A :class:`CupInputs`.
    """
    from sportsdataverse.nba.nba_season_sim import _records

    cup = cup_games(cup_schedule, as_of_date=as_of_date)
    rel = games.select("game_id", pl.col("home_pts").alias("rel_h"), pl.col("away_pts").alias("rel_a"))
    assert rel.schema["game_id"] == cup.schema["game_id"], "game_id dtype differs between the release and the Cup"
    in_rel = pl.col("game_id").is_in(rel.get_column("game_id").implode())
    cup = (
        cup.join(rel, on="game_id", how="left", validate="1:1")
        .with_columns(
            pl.when(in_rel).then(pl.col("rel_h")).otherwise(pl.col("home_pts")).alias("home_pts"),
            pl.when(in_rel).then(pl.col("rel_a")).otherwise(pl.col("away_pts")).alias("away_pts"),
        )
        .drop("rel_h", "rel_a")
    )
    group_ids = cup.filter(pl.col("cup_round") == "group").get_column("game_id").implode()
    ot = overtime_points(possessions.filter(pl.col("game_id").cast(pl.Utf8).is_in(group_ids)))
    prior = None if last_games is None or last_games.height == 0 else _records(last_games)
    return prepare_cup(cup, ot_points=ot, prior_records=prior)


def fetch_cup_schedule(season: int) -> pl.DataFrame:
    """The season's ``scheduleleaguev2`` rows from stats.nba.com (residential connection).

    Raises:
        ValueError: If ``season`` is before the first Cup (2024).
    """
    from sportsdataverse.nba.nba_stats import nba_stats_scheduleleaguev2

    if season < CUP_FIRST_SEASON:
        raise ValueError(f"the first NBA Cup is season {CUP_FIRST_SEASON} (2023-24), got {season}")
    return nba_stats_scheduleleaguev2(league_id="00", season=_season_label(season))


def _load_cup(season: int, as_of_date: dt.date | None, cup_schedule: pl.DataFrame | None) -> CupInputs:
    """:class:`CupInputs` from stats.nba.com's schedule and the possessions / schedules releases."""
    from sportsdataverse.nba.nba_season_sim import _regular_games
    from sportsdataverse.nba.nba_team_season_rates import load_nba_stats_possessions, load_nba_stats_schedules

    if season < CUP_FIRST_SEASON:
        raise ValueError(f"the first NBA Cup is season {CUP_FIRST_SEASON} (2023-24), got {season}")
    sched = cup_schedule if cup_schedule is not None else fetch_cup_schedule(season)
    return cup_inputs(
        sched,
        games=_regular_games(load_nba_stats_schedules([season - 1]), as_of_date),
        possessions=load_nba_stats_possessions([season - 1]),
        last_games=_regular_games(load_nba_stats_schedules([season - 2]), None),
        as_of_date=as_of_date,
    )


@overload
def nba_cup_standings(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    cup_schedule: pl.DataFrame | None = None,
    seed: int = 20261103,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def nba_cup_standings(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    cup_schedule: pl.DataFrame | None = None,
    seed: int = 20261103,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nba_cup_standings(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    cup_schedule: pl.DataFrame | None = None,
    seed: int = 20261103,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """NBA Cup Group Play standings as of a date, with the official tiebreak order and wild cards.

    The rules (NBA.com, "Emirates NBA Cup 101", https://www.nba.com/news/nba-cup-101, read
    2026-11-04) are in :mod:`sportsdataverse.nba.nba_cup`: Group Play record, then head-to-head,
    point differential and points scored in Group Play (overtime excluded), the previous
    regular-season record and a random drawing; the wild card is the best second-place team of
    each conference by the same order without head-to-head.

    Args:
        season: The season, named by the year it ends (``2027`` is 2026-27); from 2024.
        as_of_date: Games on or after this date are unplayed. None: every final game counts.
        cup_schedule: A :func:`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` frame
            for the season. None fetches it from stats.nba.com (a residential connection: the
            site hangs on datacenter IPs).
        seed: Seeds the random drawing, the last tiebreaker.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per team (stats.nba.com ``team_id``, Int64), sorted by conference, group and rank:
        ``group_rank`` with ``wins`` / ``losses`` / ``games_remaining``, the tiebreak inputs
        ``point_diff`` / ``points`` (overtime excluded) / ``prior_win_pct``, ``tiebreak`` (the
        criterion that placed the team among those with its record; null when the record alone
        did), ``conference_rank`` (the group leaders 1-3, then the rest, the wild-card race),
        ``leads_group``, ``holds_wild_card`` and ``knockout_seed`` (1-4 on the current table).

    Raises:
        ValueError: If ``season`` is before 2024 or the schedule does not hold six groups of five.

    Example:
        The 2025-26 Cup at the end of Group Play::

            from sportsdataverse.nba.nba_cup import nba_cup_standings
            nba_cup_standings(2026).filter(pl.col("knockout_seed").is_not_null())

    See Also:
        * :func:`nba_cup_sim` -- the odds to advance and win
        * :func:`sportsdataverse.nba.nba_season_sim.nba_season_sim` -- the season sim
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    cup = _load_cup(season, as_of_date, cup_schedule)
    out = cup_standings(cup, as_of_date=as_of_date, seed=seed)
    return out.to_pandas() if return_as_pandas else out


@overload
def nba_cup_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    n_sims: int = 10_000,
    seed: int = 20261103,
    cup_schedule: pl.DataFrame | None = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def nba_cup_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    n_sims: int = 10_000,
    seed: int = 20261103,
    cup_schedule: pl.DataFrame | None = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nba_cup_sim(
    season: int,
    *,
    as_of_date: dt.date | None = None,
    n_sims: int = 10_000,
    seed: int = 20261103,
    cup_schedule: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """NBA Cup sim as of a date: odds to win the group, take a wild card, reach each round and win.

    Runs the season sim (:func:`sportsdataverse.nba.nba_season_sim.nba_season_sim`: the as-of
    possession ridge with the preseason prior, one rating shift per team per simulated season, the
    same seed and draws) with the Cup inside it, ranks every simulated group with the Cup
    tiebreakers (:func:`nba_cup_standings`), and plays the knockout: seed 1 hosts the wild card
    and seed 2 seed 3 in each conference, the semifinals neutral through 2025-26 and at the
    higher seed from 2026-27, the final neutral. A knockout game already played keeps its result.

    Args:
        season: The season, named by the year it ends (``2027`` is 2026-27); from 2024.
        as_of_date: Games on or after this date are simulated even when they have a score (a
            backtest). None: every final game counts as played.
        n_sims: Simulated Cups (Monte Carlo error: the ``_se`` columns).
        seed: Seeds the generator; the same data and seed give the same output.
        cup_schedule: A :func:`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` frame;
            None fetches it from stats.nba.com (residential connection).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per team (``team_id``, Int64), sorted by conference, group and mean group rank:
        the Group Play ``wins`` / ``losses`` / ``point_diff`` so far, the as-of ``adj_net_rtg``,
        ``p_win_group``, ``p_wild_card``, ``p_quarterfinal`` (either), ``p_semifinal``,
        ``p_final``, ``p_champion``, each with its Monte Carlo ``_se``
        (``sqrt(p (1 - p) / n_sims)``), ``mean_group_rank`` and ``n_sims``.

    Raises:
        ValueError: If ``season`` is before 2024 or ``n_sims < 1``.

    Example:
        The 2026-27 Cup a week into Group Play::

            import datetime as dt
            from sportsdataverse.nba.nba_cup import nba_cup_sim
            nba_cup_sim(2027, as_of_date=dt.date(2026, 11, 6)).select("team_id", "p_quarterfinal", "p_champion")

    See Also:
        * :func:`nba_cup_standings` -- the group tables and tiebreakers
        * :func:`sportsdataverse.nba.nba_season_sim.nba_season_sim` -- the season sim
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    from sportsdataverse.nba.nba_season_sim import _GAME_COLS, _load_inputs

    if season < CUP_FIRST_SEASON:
        raise ValueError(f"the first NBA Cup is season {CUP_FIRST_SEASON} (2023-24), got {season}")
    games, model, _, _, cup = _load_inputs(season, as_of_date, "00", cup_schedule=cup_schedule, cup="always")
    assert cup is not None
    games = games.select(*_GAME_COLS).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    out = simulate_cup(games, cup, model, n_sims=n_sims, seed=seed, as_of_date=as_of_date)
    return out.to_pandas() if return_as_pandas else out
