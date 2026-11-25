"""Event-level NCAA lineups: each pbp row's five on the floor, one team at a time.

The walk :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.parse_ncaa_bb_game_pbp` runs by default
(``event_lineups=True``) in place of bigballR's queue-paired, possession-frozen one. The rules it
applies, and the measurements behind them, are in :func:`_event_walk`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__: list[str] = []  # private engine; parse_ncaa_bb_game_pbp is the public surface

_SUB_TYPES = ("Enters Game", "Leaves Game")
#: Rows after which the ball is dead, so an unlogged substitution can have happened: the
#: latest of them before a player's act is where :func:`_event_walk` puts him on the floor.
_DEAD_BALL_TYPES = frozenset({"Enters Game", "Leaves Game", "Timeout", "Commits Foul", "Turnover", "Free Throw"})


class _TeamWalk:
    """One team's walk through a period's rows; :func:`_event_walk` documents the rules."""

    def __init__(
        self,
        prs: list[dict[str, Any]],
        team: str | None,
        starters: list[str],
        skip_sub: Callable[[dict[str, Any]], bool],
    ) -> None:
        self.prs, self.team, self.skip_sub = prs, team, skip_sub
        self.n = len(prs)
        self.order = self._same_second_order()
        self.pos = {j: t for t, j in enumerate(self.order)}
        # every player's subs and acts by walk position, for the look-ahead in balancing and repair
        self.timeline: dict[str, list[tuple[int, str]]] = {}
        for t, j in enumerate(self.order):
            r = prs[j]
            if self.own_sub(r):
                kind = "in" if r["event_type"] == "Enters Game" else "out"
                self.timeline.setdefault(r["player_1"], []).append((t, kind))
            elif (a := self.actor(r)) is not None:
                self.timeline.setdefault(a, []).append((t, "act"))
        self.on: list[str] = list(dict.fromkeys(starters))
        self.fives: list[list[str]] = [[] for _ in range(self.n)]
        self.before: dict[Any, list[str]] = {}  # each clock reading's five before its first row
        self.moved_in: set[str] = set()
        self.moved_out: set[str] = set()

    def own_sub(self, r: dict[str, Any]) -> bool:
        return (
            r["event_team"] == self.team
            and r["event_type"] in _SUB_TYPES
            and bool(r["player_1"])
            and not self.skip_sub(r)
        )

    def actor(self, r: dict[str, Any]) -> str | None:
        if r["event_team"] != self.team or r["event_type"] in _SUB_TYPES or r["player_1"] in (None, "TEAM"):
            return None
        return r["player_1"]

    def _same_second_order(self) -> list[int]:
        """Row indices in walk order: a player's own sub moves past his own acts in that clock reading."""
        first: dict[tuple[Any, str], int] = {}
        last: dict[tuple[Any, str], int] = {}
        for j, r in enumerate(self.prs):
            a = self.actor(r)
            if a is not None:
                first.setdefault((r["game_seconds"], a), j)
                last[(r["game_seconds"], a)] = j
        key: list[float] = []
        for j, r in enumerate(self.prs):
            k: float = j
            if self.own_sub(r):
                sk = (r["game_seconds"], r["player_1"])
                if r["event_type"] == "Leaves Game" and last.get(sk, -1) > j:
                    k = last[sk] + 0.5
                elif r["event_type"] == "Enters Game" and first.get(sk, self.n) < j:
                    k = first[sk] - 0.5
            key.append(k)
        return sorted(range(self.n), key=lambda j: key[j])

    def next_event(self, p: str, t: int) -> tuple[int, str | None]:
        return next(((tt, kd) for tt, kd in self.timeline.get(p, ()) if tt > t), (self.n, None))

    def last_sub(self, p: str, t: int) -> int:
        return max((tt for tt, kd in self.timeline.get(p, ()) if tt < t and kd != "act"), default=-1)

    def surplus_rank(self, p: str, t: int, lo: int) -> tuple[int, int]:
        """Lowest first: the likeliest player NOT on the floor over walk positions ``lo``..``t``."""
        if any(lo <= tt <= t and kd == "act" for tt, kd in self.timeline.get(p, ())):
            return (4, 0)  # he played in the span: never the one to take off
        tt, kd = self.next_event(p, t)
        return ({"in": 0, None: 1, "act": 2, "out": 3}[kd], -tt)

    def put_on(self, p: str, lo: int, t: int) -> None:
        """``p`` was on from walk position ``lo`` + 1 through ``t``: replace the likeliest absentee."""
        evict = None
        if len(self.on) >= 5:
            cands = [x for x in self.on if x not in self.moved_in]
            if cands:
                evict = min(cands, key=lambda x: self.surplus_rank(x, t, lo + 1))
                self.on.remove(evict)
        self.on.append(p)
        for f in self.fives[lo + 1 : t]:
            if p not in f:
                if evict is not None and evict in f:
                    f[f.index(evict)] = p
                elif len(f) < 5:
                    f.append(p)

    def balance(self, t: int) -> None:
        """End of a clock reading: back to five on the look-ahead (see :func:`_event_walk`)."""
        while len(self.on) > 5:
            cands = [x for x in self.on if x not in self.moved_in] or list(self.on)
            self.on.remove(min(cands, key=lambda x: self.surplus_rank(x, t, t + 1)))
        if len(self.on) < 5:
            rank = {"out": 0, "act": 1}
            off = (x for x in self.timeline if x not in self.on and x not in self.moved_out)
            for p in sorted(off, key=lambda x: (rank.get(self.next_event(x, t)[1] or "", 9), self.next_event(x, t)[0])):
                if len(self.on) == 5 or self.next_event(p, t)[1] not in rank:
                    break
                self.on.append(p)
        self.moved_in.clear()
        self.moved_out.clear()

    def walk(self) -> None:
        dead = -1  # walk position of the latest dead ball
        sec_dead = -1  # ... as of the start of the current clock reading
        for t, j in enumerate(self.order):
            r = self.prs[j]
            et = r["event_type"]
            self.before.setdefault(r["game_seconds"], list(self.on))
            if self.own_sub(r):
                p = r["player_1"]
                if et == "Leaves Game":
                    if p not in self.on:  # he left without having come on: on since the dead ball before
                        self.put_on(p, max(sec_dead, self.last_sub(p, t)), t)
                    self.on.remove(p)
                    self.moved_out.add(p)
                elif p not in self.on:
                    self.on.append(p)
                    self.moved_in.add(p)
            elif (a := self.actor(r)) is not None and a not in self.on and et != "Free Throw":
                self.put_on(a, max(dead, self.last_sub(a, t)), t)
            if et in _DEAD_BALL_TYPES:
                dead = t
            nxt = self.order[t + 1] if t + 1 < self.n else None
            if nxt is None or self.prs[nxt]["game_seconds"] != r["game_seconds"]:
                self.balance(t)
                sec_dead = dead
            self.fives[t] = list(self.on)

    def rows(self) -> list[list[str | None]]:
        """One five per row in page order.

        A sub row, and the clock reading's last row (which holds the five until the clock moves),
        carry the reading's balanced five. Any other row caught between two of the reading's subs
        with a team not at five sees one of the two real states: the subs of a dead ball go in at
        one moment (Rule 3-6.2), so the five before the reading or the five after it, whichever
        holds its actor; when neither does, the walk's own state (which has him).
        """
        final: dict[Any, int] = {}  # clock reading -> walk position of its last row
        last_j: dict[Any, int] = {}  # clock reading -> its last row in page order
        for j, r in enumerate(self.prs):
            last_j[r["game_seconds"]] = j
            final[r["game_seconds"]] = max(final.get(r["game_seconds"], -1), self.pos[j])
        out: list[list[str | None]] = []
        for j, r in enumerate(self.prs):
            sec = r["game_seconds"]
            after, use = self.fives[final[sec]], self.fives[self.pos[j]]
            if r["event_type"] in _SUB_TYPES or (last_j[sec] == j and r["event_type"] != "Free Throw"):
                use = after
            elif len(use) != 5:
                a = self.actor(r)
                if a is None or a in after:
                    use = after
                elif a in self.before[sec]:
                    use = self.before[sec]
            five: list[str | None] = list(sorted(use)[:5])
            out.append(five + [None] * (5 - len(five)))
        return out


def _event_walk(
    prs: list[dict[str, Any]],
    team: str | None,
    starters: list[str],
    skip_sub: Callable[[dict[str, Any]], bool],
) -> list[list[str | None]]:
    """One team's five on the floor at each of a period's rows, applying every sub at its own dead ball.

    bigballR pairs the period's i-th ``Leaves Game`` with its i-th ``Enters Game`` and freezes
    each possession on its first row's lineup, so a substitution in a mid-possession dead ball
    (after a foul, at a timeout, between free throws) is lost until the next possession, and one
    unmatched sub shifts every later pairing. This walk applies each sub as a set change instead,
    on the NCAA rules (Rule 3-6.2; ``ClaudeCowork notes/2026-11-25-ncaa-bb-lineup-attribution``):

    * Within one clock reading the page prints rows out of order, and a free thrower cannot be
      replaced: a player's own ``Leaves Game`` counts after his last act of that second, his own
      ``Enters Game`` before his first.
    * After each clock reading the team is balanced to five: a surplus drops the player whose
      next sub is an ``Enters Game`` (he cannot be on) or who never acts again; a shortfall adds
      the player whose next sub is a ``Leaves Game`` (he must be on), else the next to act.
    * A player who acts while off the floor (a shot, rebound, foul, turnover ... but not a free
      throw: substitute and technical shooters are legal) came on at the latest dead ball before
      the act, unlogged: he goes on from there, replacing the player least likely to be on.

    Args:
        prs: The period's rows, in the parser's arranged order.
        team: The team whose lineup to walk.
        starters: Its five at the period's start (:func:`~sportsdataverse.mbb.mbb_ncaa_game_pbp._infer_starters`).
        skip_sub: True on a sub row to ignore (a placeholder such as ``UNKNOWN.TEAM``).

    Returns:
        One five per row of ``prs``, in ``prs`` order, padded with None (at most five names).
    """
    team_walk = _TeamWalk(prs, team, starters, skip_sub)
    team_walk.walk()
    return team_walk.rows()
