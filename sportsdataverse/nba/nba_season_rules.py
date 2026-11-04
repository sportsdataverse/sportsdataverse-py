"""NBA / WNBA season rules (BB-D18): standings with tiebreakers, the play-in and the draft lottery.

The league rules the season sim (:mod:`sportsdataverse.nba.nba_season_sim`) applies to every
simulated season, and that :func:`nba_season_standings` applies to the games already played.
Each rule cites its source; the current rules are used for every season.

**NBA standings tiebreakers** (NBA.com standings, "Tie-Break Procedure", paragraphs L.2.a and
L.2.b, read 2026-11-03: https://www.nba.com/standings). Ties are in winning percentage within a
conference. Ties for a division lead are broken first, with the same criteria less the
division-winner one, and that result decides the division winner only.

* Two teams: (1) head-to-head winning percentage; (2) division winner, whether or not the two
  share a division; (3) division winning percentage, only if they share a division; (4)
  conference winning percentage; (5) winning percentage against postseason-eligible teams in
  their own conference; (6) the same against postseason-eligible teams in the other conference;
  (7) point differential in all games.
* More than two: (1) division winner; (2) winning percentage in all games among the tied teams;
  (3) division winning percentage, only if all are in one division; (4) conference winning
  percentage; (5) winning percentage against postseason-eligible teams in their own conference;
  (6) point differential in all games.
* "Postseason-eligible" is a team with one of its conference's top 10 records, counting every
  team tied for one of them. A criterion that splits the tied teams sends each group that is
  still tied back to step 1, under the two-team rules when two remain. A tie that survives every
  step is a random drawing.

**NBA play-in** (NBA.com, the 2023-24 rule kept since): in each conference the 7th seed hosts the
8th, the winner is the 7th playoff seed; the 9th hosts the 10th, the loser is out; the loser of
7-8 hosts the winner of 9-10 for the 8th seed. Seeds 1-6 go straight to the playoffs.

**NBA draft lottery** (the 2019 reform, NBA.com "NBA Draft Lottery: How it works"): the 14 teams
that miss the playoffs (the play-in losers included) are ordered by record, worst first, and the
top four picks are drawn with 140, 140, 140, 125, 105, 90, 75, 60, 45, 30, 20, 15, 10 and 5
chances in 1,000 (:data:`NBA_LOTTERY_CHANCES`). Picks 5-14 follow the remaining order. Teams with
the same record split their chances evenly and their order is a random drawing.

**WNBA standings** (Wikipedia, "WNBA playoffs", the current procedure, read 2026-11-03; WNBA.com
publishes no rulebook page): one table for the league, conferences play no part in seeding.
Ties: (1) winning percentage in games among the tied teams; (2) winning percentage against all
teams at .500 or better at the end of the season; (3) point differential in games among the
tied teams; (4) point differential in all games; (5) a coin toss or a draw of lots. A criterion
that splits the tie restarts at step 1 for each group still tied, as the NBA does. The top eight
make the playoffs (best-of-three first round, the top four at home), with no play-in. This is
the 2025 format, kept for the 2026 15-team league (Portland Fire and Toronto Tempo joined).

**WNBA draft lottery** (WNBA.com "WNBA Draft Lottery 2026: FAQ", read 2026-11-03): every team that
missed the playoffs is in, ordered by its combined record over the two latest regular seasons,
worst first (an expansion team counts its seasons played). Two picks are drawn, and the rest go
by that order. The published chances are 442 / 276 / 178 / 104 for four teams and
420 / 261 / 167 / 97 / 55 for five (:data:`WNBA_LOTTERY_CHANCES`). The 2027 lottery has seven
teams and its chances are not published yet: pass them in, or the lottery odds stay null. Tied
teams share their chances evenly; their order is the current season's record, then a drawing.

Ignored on purpose: traded and swapped picks (the odds belong to the slot, not the owner), the
NBA's 2025-26 lottery-reform proposals, and the NBA Cup final (not a regular-season game).

**Internal** helpers plus two public functions, :func:`nba_season_standings` and
:func:`nba_draft_lottery_odds`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Literal, Union, overload

import numpy as np
import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_draft_lottery_odds", "nba_season_standings"]

#: A tiebreak criterion: one value per team of the group (higher is better), or None to skip it.
Criterion = Callable[[Sequence[int]], Union[list[float], None]]

#: stats.nba.com team id -> (conference, division). The six-division alignment of 2004-05 onward,
#: unchanged since (franchise ids survive relocations and renames).
NBA_DIVISIONS: dict[int, tuple[str, str]] = {
    1610612738: ("East", "Atlantic"),  # BOS
    1610612751: ("East", "Atlantic"),  # BKN
    1610612752: ("East", "Atlantic"),  # NYK
    1610612755: ("East", "Atlantic"),  # PHI
    1610612761: ("East", "Atlantic"),  # TOR
    1610612741: ("East", "Central"),  # CHI
    1610612739: ("East", "Central"),  # CLE
    1610612765: ("East", "Central"),  # DET
    1610612754: ("East", "Central"),  # IND
    1610612749: ("East", "Central"),  # MIL
    1610612737: ("East", "Southeast"),  # ATL
    1610612766: ("East", "Southeast"),  # CHA
    1610612748: ("East", "Southeast"),  # MIA
    1610612753: ("East", "Southeast"),  # ORL
    1610612764: ("East", "Southeast"),  # WAS
    1610612743: ("West", "Northwest"),  # DEN
    1610612750: ("West", "Northwest"),  # MIN
    1610612760: ("West", "Northwest"),  # OKC
    1610612757: ("West", "Northwest"),  # POR
    1610612762: ("West", "Northwest"),  # UTA
    1610612744: ("West", "Pacific"),  # GSW
    1610612746: ("West", "Pacific"),  # LAC
    1610612747: ("West", "Pacific"),  # LAL
    1610612756: ("West", "Pacific"),  # PHX
    1610612758: ("West", "Pacific"),  # SAC
    1610612742: ("West", "Southwest"),  # DAL
    1610612745: ("West", "Southwest"),  # HOU
    1610612763: ("West", "Southwest"),  # MEM
    1610612740: ("West", "Southwest"),  # NOP
    1610612759: ("West", "Southwest"),  # SAS
}

#: The 2019 NBA lottery chances in 1,000 for the 14 slots, worst record first; four picks drawn.
NBA_LOTTERY_CHANCES: tuple[int, ...] = (140, 140, 140, 125, 105, 90, 75, 60, 45, 30, 20, 15, 10, 5)
NBA_LOTTERY_DRAWS = 4

#: Published WNBA lottery chances in 1,000 by the number of lottery teams; two picks drawn.
WNBA_LOTTERY_CHANCES: dict[int, tuple[int, ...]] = {
    4: (442, 276, 178, 104),
    5: (420, 261, 167, 97, 55),
}
WNBA_LOTTERY_DRAWS = 2
WNBA_PLAYOFF_TEAMS = 8

#: :func:`nba_season_standings` output.
STANDINGS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "conference": pl.Utf8(),
    "division": pl.Utf8(),
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "win_pct": pl.Float64(),
    "point_diff": pl.Int64(),
    "division_rank": pl.Int64(),
    "division_leader": pl.Boolean(),
    "seed": pl.Int64(),
    "games_back": pl.Float64(),
}


def lottery_chances(league_id: str, n_teams: int) -> Union[tuple[int, ...], None]:
    """The published lottery chances for ``n_teams`` lottery teams, or None when there are none.

    Args:
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        n_teams: The number of teams in the lottery.

    Returns:
        Chances in 1,000 per slot, worst record first; None when the league has published no
        table for that many teams (the 2027 WNBA lottery's seven teams).
    """
    if league_id == "00":
        return NBA_LOTTERY_CHANCES if n_teams == len(NBA_LOTTERY_CHANCES) else None
    return WNBA_LOTTERY_CHANCES.get(n_teams)


class Season:
    """One season's results among ``T`` teams, as arrays, with the league's ranking rules.

    Args:
        wins: ``(T, T)`` wins of team ``i`` over team ``j``.
        point_diff: ``(T,)`` point differential in all games.
        conference: ``(T,)`` conference labels (WNBA: one label).
        division: ``(T,)`` division labels (WNBA: one label).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        rng: The generator for drawings.
        pair_diff: ``(T, T)`` point differential of ``i`` against ``j``; WNBA only.
    """

    def __init__(
        self,
        wins: np.ndarray,
        point_diff: np.ndarray,
        conference: np.ndarray,
        division: np.ndarray,
        *,
        league_id: str,
        rng: np.random.Generator,
        pair_diff: Union[np.ndarray, None] = None,
    ) -> None:
        self.w = wins
        self.g = wins + wins.T
        self.pd = point_diff
        self.pair_diff = pair_diff
        self.conf = conference
        self.div = division
        self.league_id = league_id
        self.rng = rng
        total = self.g.sum(1)
        self.pct = np.where(total > 0, wins.sum(1) / np.maximum(total, 1), 0.0)
        self.leader: np.ndarray = np.zeros(len(point_diff), dtype=bool)
        self.eligible: np.ndarray = np.zeros(len(point_diff), dtype=bool)
        if league_id == "00":
            for c in np.unique(conference):
                members = np.flatnonzero(conference == c)
                cut = np.sort(self.pct[members])[::-1][min(9, members.size - 1)]
                self.eligible[members[self.pct[members] >= cut]] = True
            for d in np.unique(division):
                self.leader[self.division_order(np.flatnonzero(division == d))[0]] = True

    # --- criteria: each returns one value per team of ``group``, higher is better, or None to skip
    def _pct_vs(self, group: Sequence[int], opponents: np.ndarray) -> list[float]:
        out = []
        for i in group:
            g = self.g[i, opponents].sum()
            out.append(self.w[i, opponents].sum() / g if g else 0.5)
        return out

    def _head_to_head(self, group: Sequence[int]) -> list[float]:
        return self._pct_vs(group, np.asarray(group))

    def _division_winner(self, group: Sequence[int]) -> list[float]:
        return [float(self.leader[i]) for i in group]

    def _division_pct(self, group: Sequence[int]) -> Union[list[float], None]:
        if len({self.div[i] for i in group}) > 1:
            return None
        return self._pct_vs(group, np.flatnonzero(self.div == self.div[group[0]]))

    def _conference_pct(self, group: Sequence[int]) -> list[float]:
        return self._pct_vs(group, np.flatnonzero(self.conf == self.conf[group[0]]))

    def _eligible_own(self, group: Sequence[int]) -> list[float]:
        return self._pct_vs(group, np.flatnonzero(self.eligible & (self.conf == self.conf[group[0]])))

    def _eligible_other(self, group: Sequence[int]) -> list[float]:
        return self._pct_vs(group, np.flatnonzero(self.eligible & (self.conf != self.conf[group[0]])))

    def _point_diff(self, group: Sequence[int]) -> list[float]:
        return [float(self.pd[i]) for i in group]

    def _vs_winning(self, group: Sequence[int]) -> list[float]:
        return self._pct_vs(group, np.flatnonzero(self.pct >= 0.5))

    def _pair_point_diff(self, group: Sequence[int]) -> list[float]:
        assert self.pair_diff is not None, "the WNBA rules need pair_diff"
        return [float(self.pair_diff[i, list(group)].sum()) for i in group]

    def _criteria(self, n: int, with_leader: bool) -> list[Criterion]:
        if self.league_id == "10":
            wnba: list[Criterion] = [self._head_to_head, self._vs_winning, self._pair_point_diff, self._point_diff]
            return wnba
        if n == 2:
            two: list[Criterion] = [self._head_to_head, self._division_winner, self._division_pct, self._conference_pct]
            two += [self._eligible_own, self._eligible_other, self._point_diff]
            return two if with_leader else [c for c in two if c != self._division_winner]
        multi: list[Criterion] = [self._division_winner, self._head_to_head, self._division_pct, self._conference_pct]
        multi += [self._eligible_own, self._point_diff]
        return multi if with_leader else multi[1:]

    def break_tie(self, group: list[int], *, with_leader: bool = True) -> list[int]:
        """Order teams tied in winning percentage, best first, by the league's procedure."""
        if len(group) < 2:
            return list(group)
        for criterion in self._criteria(len(group), with_leader):
            values = criterion(group)
            if values is None or len(set(values)) == 1:
                continue
            out: list[int] = []
            for v in sorted(set(values), reverse=True):
                out += self.break_tie([t for t, x in zip(group, values) if x == v], with_leader=with_leader)
            return out
        return [group[k] for k in self.rng.permutation(len(group))]

    def order(self, members: np.ndarray, *, with_leader: bool = True) -> list[int]:
        """``members`` best first: by winning percentage, ties by :meth:`break_tie`."""
        out: list[int] = []
        pcts = self.pct[members]
        for v in np.unique(pcts)[::-1]:
            out += self.break_tie([int(t) for t in members[pcts == v]], with_leader=with_leader)
        return out

    def division_order(self, members: np.ndarray) -> list[int]:
        """A division best first; the leader is decided without the division-winner step."""
        pcts = self.pct[members]
        top = [int(t) for t in members[pcts == pcts.max()]]
        leader = self.break_tie(top, with_leader=False)[0]
        rest = members[members != leader]
        return [leader, *self.order(rest, with_leader=False)] if rest.size else [leader]

    def seeds(self) -> np.ndarray:
        """``(T,)`` seed: the rank within the conference (NBA) or the league (WNBA), 1 = best."""
        seed: np.ndarray = np.zeros(len(self.pd), dtype=np.int64)
        for c in np.unique(self.conf):
            for rank, t in enumerate(self.order(np.flatnonzero(self.conf == c)), start=1):
                seed[t] = rank
        return seed

    def division_ranks(self) -> np.ndarray:
        """``(T,)`` rank within the division, 1 = the division winner."""
        rank: np.ndarray = np.zeros(len(self.pd), dtype=np.int64)
        for d in np.unique(self.div):
            for k, t in enumerate(self.division_order(np.flatnonzero(self.div == d)), start=1):
                rank[t] = k
        return rank


def lottery_slots(
    pct: np.ndarray, tiebreak: np.ndarray, chances: Sequence[float], rng: np.random.Generator
) -> tuple[np.ndarray, np.ndarray]:
    """Pre-lottery order of the lottery teams and each slot's chances, ties shared evenly.

    Args:
        pct: ``(n,)`` the record the order is by, worst first (lower is worse).
        tiebreak: ``(n,)`` the next key for equal ``pct`` (lower goes first); then a drawing.
        chances: The published chances per slot, worst first (``len == n``).
        rng: The generator for the drawing.

    Returns:
        ``(order, slot_chances)``: indexes into ``pct`` worst first, and the chances of each slot
        with every group of equal ``pct`` given the mean of its slots' chances. ``tiebreak`` only
        orders a tied group (the WNBA's current-season record); it does not split the chances.
    """
    order = np.lexsort((rng.random(pct.size), tiebreak, pct))
    keys = pct[order].tolist()
    out: np.ndarray = np.asarray(chances, dtype=float).copy()
    k = 0
    while k < len(keys):
        j = k
        while j + 1 < len(keys) and keys[j + 1] == keys[k]:
            j += 1
        out[k : j + 1] = out[k : j + 1].mean()
        k = j + 1
    return order, out


def draw_picks(slot_chances: np.ndarray, draws: int, rng: np.random.Generator) -> np.ndarray:
    """Draft pick per slot for many lotteries at once: ``draws`` weighted draws, then by slot.

    Sequential weighted draws without replacement are the order of the keys ``log(u) / w``
    (Efraimidis and Spirakis, 2006), so a whole batch is one sort.

    Args:
        slot_chances: ``(S, n)`` chances per slot for ``S`` lotteries (rows need not sum to 1).
        draws: How many picks are drawn.
        rng: The generator.

    Returns:
        ``(S, n)`` the pick (1-based) each slot gets.
    """
    s, n = slot_chances.shape
    keys = np.log(rng.random((s, n))) / slot_chances
    drawn = np.argsort(-keys, axis=1)[:, :draws]
    picks = np.zeros((s, n), dtype=np.int64)
    rows = np.arange(s)[:, None]
    picks[rows, drawn] = np.arange(1, draws + 1)
    rest = picks == 0
    # the undrawn slots keep their order: rank among the undrawn, after the drawn picks
    picks[rest] = (np.cumsum(rest, axis=1) + draws)[rest]
    return picks


def exact_lottery_odds(chances: Sequence[float], draws: int) -> np.ndarray:
    """``(n, n)`` exact probability that slot ``i`` gets pick ``j + 1``, by enumerating draws."""
    w: np.ndarray = np.asarray(chances, dtype=float)
    n = w.size
    out = np.zeros((n, n))

    def rec(drawn: tuple[int, ...], p: float) -> None:
        if len(drawn) == draws:
            rest = [i for i in range(n) if i not in drawn]
            for k, i in enumerate([*drawn, *rest]):
                out[i, k] += p
            return
        left = float(w.sum() - w[list(drawn)].sum())
        for i in range(n):
            if i not in drawn:
                rec((*drawn, i), p * w[i] / left)

    rec((), 1.0)
    return out


@overload
def nba_draft_lottery_odds(
    *, league_id: str = "00", chances: Union[Sequence[float], None] = None, return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def nba_draft_lottery_odds(
    *, league_id: str = "00", chances: Union[Sequence[float], None] = None, return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...
def nba_draft_lottery_odds(
    *, league_id: str = "00", chances: Union[Sequence[float], None] = None, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Exact draft-lottery odds by pre-lottery slot: the chance each slot gets each pick.

    NBA: the 2019 odds, 14 slots, four picks drawn. WNBA: two picks drawn, with the published
    chances for the number of lottery teams. Every ordered draw is enumerated (24,024 for the
    NBA), so the odds are exact, not simulated.

    Args:
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        chances: Chances per slot, worst record first. Default: :data:`NBA_LOTTERY_CHANCES`
            (NBA) or the five-team :data:`WNBA_LOTTERY_CHANCES` table (WNBA, the 2026 lottery).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(lottery_slot, pick)``: ``lottery_slot`` (1 = the worst record),
        ``chances`` (the slot's chances), ``pick`` and ``probability``.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``chances`` has a
            non-positive entry or fewer entries than picks drawn.

    Example:
        The NBA table::

            from sportsdataverse.nba.nba_season_rules import nba_draft_lottery_odds
            odds = nba_draft_lottery_odds()
            odds.filter(pl.col("lottery_slot") == 1)

        A WNBA lottery with its own chances::

            nba_draft_lottery_odds(league_id="10", chances=[442, 276, 178, 104])
    """
    if league_id not in ("00", "10"):
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    draws = NBA_LOTTERY_DRAWS if league_id == "00" else WNBA_LOTTERY_DRAWS
    w = (
        list(chances)
        if chances is not None
        else list(NBA_LOTTERY_CHANCES if league_id == "00" else WNBA_LOTTERY_CHANCES[5])
    )
    if len(w) < draws or min(w) <= 0:
        raise ValueError(f"chances must be positive with at least {draws} entries, got {w}")
    odds = exact_lottery_odds(w, draws)
    n = len(w)
    out = pl.DataFrame(
        {
            "lottery_slot": np.repeat(np.arange(1, n + 1), n),
            "chances": np.repeat(np.asarray(w, dtype=float), n),
            "pick": np.tile(np.arange(1, n + 1), n),
            "probability": odds.ravel(),
        },
        schema={"lottery_slot": pl.Int64, "chances": pl.Float64, "pick": pl.Int64, "probability": pl.Float64},
    )
    return out.to_pandas() if return_as_pandas else out


def team_labels(team_ids: Sequence[int], league_id: str) -> tuple[np.ndarray, np.ndarray]:
    """``(conference, division)`` arrays for stats.nba.com team ids (WNBA: one label each).

    Raises:
        ValueError: If an NBA team id is not in :data:`NBA_DIVISIONS`.
    """
    if league_id == "10":
        return np.full(len(team_ids), "WNBA", dtype=object), np.full(len(team_ids), "WNBA", dtype=object)
    unknown = [t for t in team_ids if t not in NBA_DIVISIONS]
    if unknown:
        raise ValueError(f"team ids not in NBA_DIVISIONS (not an NBA franchise?): {unknown}")
    labels = [NBA_DIVISIONS[t] for t in team_ids]
    return np.array([c for c, _ in labels], dtype=object), np.array([d for _, d in labels], dtype=object)


def results_arrays(games: pl.DataFrame, index: dict[int, int]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """``(wins, point_diff, pair_diff)`` arrays from completed games (``home_pts``/``away_pts``)."""
    n = len(index)
    wins: np.ndarray = np.zeros((n, n), dtype=np.int64)
    pair: np.ndarray = np.zeros((n, n), dtype=np.float64)
    h = np.array([index[t] for t in games.get_column("home_team_id").to_list()], dtype=np.int64)
    a = np.array([index[t] for t in games.get_column("away_team_id").to_list()], dtype=np.int64)
    m = (games.get_column("home_pts") - games.get_column("away_pts")).to_numpy().astype(float)
    np.add.at(wins, (h, a), (m > 0).astype(np.int64))
    np.add.at(wins, (a, h), (m < 0).astype(np.int64))
    np.add.at(pair, (h, a), m)
    np.add.at(pair, (a, h), -m)
    return wins, pair.sum(1), pair


@overload
def nba_season_standings(
    games: pl.DataFrame, *, league_id: str = "00", seed: int = 0, return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def nba_season_standings(
    games: pl.DataFrame, *, league_id: str = "00", seed: int = 0, return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...
def nba_season_standings(
    games: pl.DataFrame, *, league_id: str = "00", seed: int = 0, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Standings with the league's tiebreakers from completed regular-season games.

    NBA: conference seeds and division ranks under the NBA tie-break procedure (division leaders
    first, postseason-eligible = a top-10 conference record). WNBA: one league table under the
    WNBA procedure. The rules and their sources are in :mod:`sportsdataverse.nba.nba_season_rules`.

    Args:
        games: One row per game with ``season``, ``home_team_id``, ``away_team_id`` (Int64
            stats.nba.com ids), ``home_pts`` and ``away_pts``; rows without both scores are not
            played yet and are skipped. A ``load_nba_stats_schedules`` frame can be passed as is:
            with a ``season_type`` column only the ``"regular-season"`` rows count (the NBA Cup
            final, preseason, play-in and playoffs do not).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        seed: Seeds the drawing that ends a tie no criterion breaks.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)`` that played: ``conference`` / ``division`` (NBA; the
        WNBA rows say ``"WNBA"``), ``games``, ``wins``, ``losses``, ``win_pct``, ``point_diff``,
        ``division_rank`` (1 = the division winner), ``division_leader``, ``seed`` (rank in the
        conference for the NBA, in the league for the WNBA, 1 = best) and ``games_back`` (behind
        the seed-1 team). Sorted by season, conference and seed.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or an NBA team id is unknown.
        TypeError: If the team-id columns are not Int64.

    Example:
        Final NBA standings for 2025-26::

            from sportsdataverse.nba.nba_loaders import load_nba_stats_schedules
            from sportsdataverse.nba.nba_season_rules import nba_season_standings
            nba_season_standings(load_nba_stats_schedules(2025)).filter(pl.col("seed") <= 10)
    """
    if league_id not in ("00", "10"):
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    for key in ("home_team_id", "away_team_id"):
        if games.schema[key] != pl.Int64:
            raise TypeError(f"{key} must be Int64 (stats.nba.com ids), got {games.schema[key]}")
    done = games.filter(pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null())
    if "season_type" in done.columns:
        done = done.filter(pl.col("season_type") == "regular-season")
    rng = np.random.default_rng(seed)
    frames = []
    for (season,), sub in done.group_by("season", maintain_order=True):
        teams = sorted(set(sub["home_team_id"].to_list()) | set(sub["away_team_id"].to_list()))
        index = {t: i for i, t in enumerate(teams)}
        conf, div = team_labels(teams, league_id)
        wins, pdiff, pair = results_arrays(sub, index)
        season_ = Season(wins, pdiff, conf, div, league_id=league_id, rng=rng, pair_diff=pair)
        seeds = season_.seeds()
        w, lost = wins.sum(1), wins.sum(0)
        top = {c: int(np.flatnonzero((conf == c) & (seeds == 1))[0]) for c in np.unique(conf)}
        back = [((w[top[conf[i]]] - w[i]) + (lost[i] - lost[top[conf[i]]])) / 2 for i in range(len(teams))]
        frames.append(
            pl.DataFrame(
                {
                    "season": [season] * len(teams),
                    "team_id": teams,
                    "conference": conf.tolist(),
                    "division": div.tolist(),
                    "games": (w + lost).tolist(),
                    "wins": w.tolist(),
                    "losses": lost.tolist(),
                    "win_pct": season_.pct.tolist(),
                    "point_diff": pdiff.round().astype(np.int64).tolist(),
                    "division_rank": season_.division_ranks().tolist(),
                    "division_leader": season_.leader.tolist() if league_id == "00" else [False] * len(teams),
                    "seed": seeds.tolist(),
                    "games_back": back,
                },
                schema=STANDINGS_SCHEMA,
            )
        )
    out = pl.concat(frames).sort("season", "conference", "seed") if frames else pl.DataFrame(schema=STANDINGS_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
