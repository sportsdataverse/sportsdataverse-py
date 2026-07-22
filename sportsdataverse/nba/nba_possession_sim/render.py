"""Action-level pbp rendering — provider-parameterized formulaic play text.

The expanded possession walk explodes into ACTION rows — shots, free
throws, rebounds (player + team/deadball), turnovers (player + shot
clock), steals, blocks, fouls (personal + shooting, with official
names), timeouts, substitutions, jump balls, and period/game status
rows — and every
description is assembled from the provider template registry
(``sportsdataverse._common.play_text``), so the dialect is a parameter:

* ``nba_stats`` — the stats.nba.com/stats.wnba.com ``playbyplayv3``
  conventions (NBA / WNBA / G-League): ``"{Name} {dist}' {subtype}
  ({pts} PTS) ({Assister} {n} AST)"``, ``"REBOUND (Off:o Def:d)"``,
  turnover/foul ``"(Pp.Tt)"`` counters;
* ``espn`` — the MBB/WBB sentence style: ``"{Name} made Three Point
  Jumper. Assisted by {Name}."``, ``"Foul on {Name}."``;
* any provider registered via
  :func:`sportsdataverse._common.play_text.register_provider`.

Every parenthetical is a RUNNING boxscore counter, so the rendered log is
internally consistent: parsing the text back reproduces the simulated
boxscore. Player names come from the same real feeds the sims classify
(:func:`player_names_from_pbp` for stats v3,
:func:`player_names_from_espn` for ESPN leagues).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse._common.play_text import get_templates
from sportsdataverse.nba.nba_possession_sim.attribution import PlayerAttribution
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import simulate_possession_expanded
from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.rules import NBA_RULES, SportRules
from sportsdataverse.nba.nba_possession_sim.shelf import Shelf

_DISTANCE = {"rim": (0, 4), "mid": (6, 21), "three": (23, 29)}
# ponytail: dead-ball officiating rates fitted from the 3 v3 fixture games —
# 1 violation and 5 instant replays over ~600 possessions
VIOLATION_RATE = 0.002
REPLAY_RATE = 0.008
_REPLAY_RULINGS = (
    "Support Ruling",
    "Support Ruling",
    "Support Ruling",
    "Overturn Ruling",
    "Coach Challenge Overturn Ruling",
)


def player_names_from_pbp(actions: pl.DataFrame) -> Dict[int, str]:
    """``{person_id: last name}`` from stats ``playbyplayv3`` action rows.

    Args:
        actions: Raw v3 action rows (``personId``/``playerName`` or
            snake_case).

    Returns:
        The name map (ambiguous ids keep their first-seen name).
    """
    pid_col = "personId" if "personId" in actions.columns else "person_id"
    name_col = "playerName" if "playerName" in actions.columns else "player_name"
    names: Dict[int, str] = {}
    for row in (
        actions.select(pl.col(pid_col).alias("pid"), pl.col(name_col).cast(pl.Utf8).alias("nm"))
        .filter((pl.col("pid") > 0) & pl.col("nm").is_not_null())
        .unique()
        .to_dicts()
    ):
        names.setdefault(int(row["pid"]), str(row["nm"]))
    return names


def player_names_from_espn(summary: Dict[str, Any]) -> Dict[int, str]:
    """``{athlete_id: display name}`` from an ESPN summary's boxscore.

    Args:
        summary: Site v2 ``summary`` with ``boxscore.players``.

    Returns:
        The name map.
    """
    names: Dict[int, str] = {}
    for team in (summary.get("boxscore") or {}).get("players") or []:
        for stat_block in team.get("statistics") or []:
            for entry in stat_block.get("athletes") or []:
                athlete = entry.get("athlete") or {}
                raw = str(athlete.get("id") or "")
                if raw.isdigit():
                    names.setdefault(int(raw), str(athlete.get("displayName") or f"#{raw}"))
    return names


def officials_from_pbp(actions: pl.DataFrame) -> List[str]:
    """Referee names parsed from v3 foul-row description suffixes.

    Args:
        actions: Raw v3 action rows (``description`` present).

    Returns:
        Sorted unique official names (``"J.Tiven"`` style) — the trailing
        parenthetical the feed stamps on foul rows.
    """
    if "description" not in actions.columns:
        return []
    extracted = (
        actions.select(
            pl.col("description").cast(pl.Utf8).str.extract(r"\(([A-Z]\.[A-Za-z'\- ]+)\)$", 1).alias("official")
        )
        .filter(pl.col("official").is_not_null())
        .filter(pl.col("official").str.contains(r"FOUL|Foul").not_())["official"]
        .unique()
        .to_list()
    )
    return sorted(str(name) for name in extracted)


def _clock_text(seconds: float) -> str:
    minutes = int(seconds) // 60
    return f"PT{minutes:02d}M{seconds - minutes * 60:05.2f}S"


class _BoxState:
    """Running per-player/team counters behind the parentheticals."""

    def __init__(self) -> None:
        self.pts: Dict[int, int] = {}
        self.oreb: Dict[int, int] = {}
        self.dreb: Dict[int, int] = {}
        self.ast: Dict[int, int] = {}
        self.tov: Dict[int, int] = {}
        self.pf: Dict[int, int] = {}
        self.stl: Dict[int, int] = {}
        self.blk: Dict[int, int] = {}
        self.team_tov = {True: 0, False: 0}
        self.team_fouls = {True: 0, False: 0}
        self.timeouts = {True: 0, False: 0}

    def bump(self, table: Dict[int, int], pid: int, amount: int = 1) -> int:
        table[pid] = table.get(pid, 0) + amount
        return table[pid]


def simulate_game_actions(
    shelf: Shelf,
    attribution: PlayerAttribution,
    names: Dict[int, str],
    rng: np.random.Generator,
    *,
    rules: SportRules = NBA_RULES,
    team_labels: Tuple[str, str] = ("HOME", "AWAY"),
    factors: Optional[FactorAdjustment] = None,
    provider: str = "nba_stats",
    officials: Optional[List[str]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
    """Simulate one game and render every action with provider play text.

    Args:
        shelf: The PMF shelf (empirical or model-backed).
        attribution: Player shares (scorer / assister / rebounder / tov).
        names: ``{player_id: name}`` (see the two builders above).
        rng: Numpy generator. Split internally into a GAME stream (score
            path) and a TEXT stream (cosmetic draws), so the same seed
            simulates the identical game under every provider dialect and
            text-only options (``officials``) never perturb outcomes.
        rules: League clock structure.
        team_labels: (home, away) labels for team-level actions.
        factors: Optional auditable PMF adjustment.
        provider: Template provider — ``"nba_stats"`` (default; NBA/WNBA/
            G-League v3 conventions), ``"espn"`` (MBB/WBB sentence style),
            or any provider registered in
            ``sportsdataverse._common.play_text`` (aliases ``v3`` /
            ``espn_college`` resolve).
        officials: Optional referee names (see :func:`officials_from_pbp`);
            when given, providers with an ``official_suffix`` template
            stamp a sampled official on every foul row (the v3 style).

    Returns:
        ``(final, actions)`` — ``final`` has ``score_home``/``score_away``;
        ``actions`` is one row per rendered action: ``action_number``,
        ``period``, ``clock_seconds``, ``clock`` (v3 ``PT..M..S`` form),
        ``action_type``, ``sub_type``, ``person_id``, ``player_name``,
        ``offense_is_home``, ``shot_value``, ``description``, and the
        running ``score_home``/``score_away``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_possession_sim.render import (
                player_names_from_pbp, simulate_game_actions,
            )
            final, actions = simulate_game_actions(
                shelf, att, player_names_from_pbp(raw_actions),
                np.random.default_rng(7), provider="nba_stats",
            )
            print(actions[0]["description"])
    """
    tpl = get_templates("basketball", provider)
    # Split the caller's generator into two independent streams: the GAME
    # stream drives the score path (tip, possession outcomes, clock burn)
    # and the TEXT stream drives everything cosmetic (officials, replay
    # rulings, flavor counters, name/attribution picks, distances). Text
    # draws are template-key-gated, so on a shared stream the draw COUNT
    # would depend on the provider and the same seed would produce a
    # different game per dialect. `rng` is rebound to the text stream so
    # any future draw lands there by default — only score-path sites may
    # touch `game_rng`.
    stream_seeds = rng.integers(0, 2**63 - 1, size=2)
    game_rng = np.random.default_rng(int(stream_seeds[0]))
    rng = np.random.default_rng(int(stream_seeds[1]))
    subtypes: Dict[str, str] = tpl["subtypes"]
    box = _BoxState()
    actions: List[Dict[str, Any]] = []
    score = {"home": 0, "away": 0}
    period, clock = 1, rules.period_seconds
    offense_is_home = bool(game_rng.random() < 0.5)
    period_unit = "half" if rules.periods == 2 else "Quarter"
    wall_minutes = 19 * 60 + 30  # synthetic 7:30 PM ET tip behind the v3 period rows

    def _name(pid: int) -> str:
        return names.get(pid, f"#{pid}")

    def _ordinal(n: int) -> str:
        return f"{n}{({1: 'st', 2: 'nd', 3: 'rd'}).get(n, 'th')}"

    def _wall_text(minutes: int) -> str:
        hour = (minutes // 60) % 12 or 12
        return f"{hour}:{minutes % 60:02d} PM"

    def _label(is_home: bool) -> str:
        return team_labels[0] if is_home else team_labels[1]

    def _emit(
        action_type: str,
        sub_type: str,
        description: str,
        *,
        pid: int = 0,
        shot_value: int = 0,
    ) -> None:
        actions.append(
            {
                "action_number": len(actions) + 1,
                "period": period,
                "clock_seconds": round(clock, 1),
                "clock": _clock_text(max(0.0, clock)),
                "action_type": action_type,
                "sub_type": sub_type,
                "person_id": pid,
                "player_name": _name(pid) if pid else "",
                "offense_is_home": offense_is_home,
                "shot_value": shot_value,
                "description": description,
                "score_home": score["home"],
                "score_away": score["away"],
            }
        )

    subs_enabled = ("substitution" in tpl) or ("sub_in" in tpl)
    usage: Dict[bool, Dict[int, float]] = {}
    on_floor: Dict[bool, List[int]] = {}
    last_actor: Dict[bool, int] = {True: 0, False: 0}
    for side_is_home, team in ((True, attribution.home), (False, attribution.away)):
        weight = team.two_shares + team.three_shares + team.ft_shares + team.tov_shares
        usage[side_is_home] = {pid: float(weight[i]) for i, pid in enumerate(team.player_ids)}
        order = np.argsort(-weight)
        on_floor[side_is_home] = [team.player_ids[int(i)] for i in order[:5]]

    def _ensure_on_floor(side_is_home: bool, pid: int) -> None:
        # ponytail: lazy rotation — an off-floor actor is subbed in on demand
        # right before acting (the v3 fixtures average ~55 SUB rows per game)
        if pid <= 0:
            return
        if not subs_enabled or pid in on_floor[side_is_home]:
            last_actor[side_is_home] = pid
            return
        floor = on_floor[side_is_home]
        if not floor:
            return
        candidates = [p for p in floor if p != last_actor[side_is_home]] or floor
        outgoing = min(candidates, key=lambda p: usage[side_is_home].get(p, 0.0))
        floor[floor.index(outgoing)] = pid
        if "sub_out" in tpl:
            _emit(
                "Substitution",
                "out",
                tpl["sub_out"].format(name=_name(outgoing), label=_label(side_is_home)),
                pid=outgoing,
            )
            _emit(
                "Substitution",
                "in",
                tpl["sub_in"].format(name=_name(pid), label=_label(side_is_home)),
                pid=pid,
            )
        else:
            _emit(
                "Substitution",
                "",
                tpl["substitution"].format(incoming=_name(pid), outgoing=_name(outgoing)),
                pid=pid,
            )
        last_actor[side_is_home] = pid

    def _official_suffix() -> str:
        if officials and "official_suffix" in tpl:
            return tpl["official_suffix"].format(official=officials[int(rng.integers(0, len(officials)))])
        return ""

    def _foul(fouler: int, *, shooting: bool) -> None:
        _ensure_on_floor(not offense_is_home, fouler)
        personal = box.bump(box.pf, fouler)
        box.team_fouls[not offense_is_home] += 1
        key = "foul_shooting" if shooting and "foul_shooting" in tpl else "foul"
        _emit(
            "Foul",
            "Shooting" if shooting else "Personal",
            tpl[key].format(
                name=_name(fouler),
                personal=personal,
                team=box.team_fouls[not offense_is_home],
                label=_label(not offense_is_home),
            )
            + _official_suffix(),
            pid=fouler,
        )

    def _period_row(kind: str) -> None:
        nonlocal wall_minutes
        key = f"period_{kind}"
        if key not in tpl:
            return
        wall_minutes += int(rng.integers(1, 4)) if kind == "start" else 26 + int(rng.integers(0, 5))
        _emit(
            "period",
            kind,
            tpl[key].format(ordinal=_ordinal(period), wall_time=_wall_text(wall_minutes), unit=period_unit),
        )

    def _render_free_throws(shooter: int, made: int, total: int) -> None:
        for attempt in range(1, total + 1):
            if attempt <= made:
                score["home" if offense_is_home else "away"] += 1
                pts = box.bump(box.pts, shooter)
                description = tpl["ft_make"].format(
                    name=_name(shooter), attempt=attempt, total=total, pts=pts, label=_label(offense_is_home)
                )
            else:
                description = tpl["ft_miss"].format(
                    name=_name(shooter), attempt=attempt, total=total, label=_label(offense_is_home)
                )
            _emit(
                "Free Throw",
                f"Free Throw {attempt} of {total}",
                description,
                pid=shooter,
                shot_value=1,
            )

    while True:
        _period_row("start")
        if period == 1 and "jump_ball" in tpl:
            home_ids, away_ids = attribution.home.player_ids, attribution.away.player_ids
            center_home = home_ids[int(np.argmax(attribution.home.reb_shares))] if home_ids else 0
            center_away = away_ids[int(np.argmax(attribution.away.reb_shares))] if away_ids else 0
            winner = attribution.home if offense_is_home else attribution.away
            tip_to = winner.sample_rebounder(rng) if winner.player_ids else 0
            _emit(
                "Jump Ball",
                "",
                tpl["jump_ball"].format(
                    center_home=_name(center_home),
                    center_away=_name(center_away),
                    tip_to=_name(tip_to),
                    label=team_labels[0] if offense_is_home else team_labels[1],
                ),
                pid=tip_to,
            )
        while clock > 0:
            if "violation" in tpl and rng.random() < VIOLATION_RATE:
                side_is_home = bool(rng.random() < 0.5)
                _emit(
                    "Violation",
                    "Delay Of Game",
                    tpl["violation"].format(
                        name=team_labels[0] if side_is_home else team_labels[1], kind="Delay of game"
                    ),
                )
            if "instant_replay" in tpl and rng.random() < REPLAY_RATE:
                elapsed = int((rules.period_seconds - max(0.0, clock)) / 60)
                _emit(
                    "Instant Replay",
                    _REPLAY_RULINGS[int(rng.integers(0, len(_REPLAY_RULINGS)))],
                    tpl["instant_replay"].format(
                        ordinal=_ordinal(period), wall_time=_wall_text(wall_minutes + elapsed)
                    ),
                )
            diff = float(score["home"] - score["away"] if offense_is_home else score["away"] - score["home"])
            _, trail, _ = simulate_possession_expanded(
                shelf,
                score_diff=diff,
                period=period,
                clock_seconds=clock,
                rng=game_rng,
                factors=factors,
                offense_is_home=offense_is_home,
            )
            offense = attribution.home if offense_is_home else attribution.away
            defense = attribution.away if offense_is_home else attribution.home
            shooter = 0
            attempt_type = ""
            index = 0
            while index < len(trail):
                token = trail[index]
                if token == "side:timeout":
                    box.timeouts[offense_is_home] += 1
                    label = team_labels[0] if offense_is_home else team_labels[1]
                    # ponytail: 6 of 14 ESPN fixture timeouts are team-charged;
                    # ~1 in 4 stats.ncaa.org fixture timeouts is the media break
                    if "timeout_team" in tpl and rng.random() < 6 / 14:
                        description = tpl["timeout_team"].format(label=label)
                    elif "timeout_media" in tpl and rng.random() < 0.25:
                        description = tpl["timeout_media"].format(label=label)
                    else:
                        description = tpl["timeout"].format(label=label, count=box.timeouts[offense_is_home])
                    _emit("Timeout", "Regular", description)
                elif token == "side:def_foul":
                    _foul(defense.sample_rebounder(rng), shooting=False)
                elif token.endswith("_attempt"):
                    attempt_type = token.rsplit("_", 1)[0]
                    shooter = offense.sample("three_make" if attempt_type == "three" else "rim_make", rng)
                    _ensure_on_floor(offense_is_home, shooter)
                elif token in ("rim_make", "mid_make", "three_make"):
                    value = 3 if attempt_type == "three" else 2
                    score["home" if offense_is_home else "away"] += value
                    pts = box.bump(box.pts, shooter, value)
                    lo, hi = _DISTANCE[attempt_type]
                    description = tpl["made_shot"].format(
                        name=_name(shooter),
                        distance=int(rng.integers(lo, hi + 1)),
                        subtype=subtypes[attempt_type],
                        pts=pts,
                        label=_label(offense_is_home),
                    )
                    assister = offense.sample_assister(shooter, rng)
                    if assister is not None and "assist_suffix" in tpl:
                        _ensure_on_floor(offense_is_home, assister)
                        description += tpl["assist_suffix"].format(
                            assister=_name(assister), ast=box.bump(box.ast, assister)
                        )
                    _emit(
                        "Made Shot",
                        subtypes[attempt_type],
                        description,
                        pid=shooter,
                        shot_value=value,
                    )
                    if assister is not None and "assist_row" in tpl:
                        _ensure_on_floor(offense_is_home, assister)
                        box.bump(box.ast, assister)
                        _emit(
                            "Assist",
                            "",
                            tpl["assist_row"].format(name=_name(assister), label=_label(offense_is_home)),
                            pid=assister,
                        )
                    if index + 2 < len(trail) and trail[index + 2] == "and1":
                        made = int(trail[index + 3].rsplit("_", 1)[1])
                        _foul(defense.sample_rebounder(rng), shooting=True)
                        _render_free_throws(shooter, made, 1)
                        index += 3
                elif token in ("rim_miss", "mid_miss", "three_miss"):
                    lo, hi = _DISTANCE[attempt_type]
                    _emit(
                        "Missed Shot",
                        subtypes[attempt_type],
                        tpl["missed_shot"].format(
                            name=_name(shooter),
                            distance=int(rng.integers(lo, hi + 1)),
                            subtype=subtypes[attempt_type],
                            label=_label(offense_is_home),
                        ),
                        pid=shooter,
                        shot_value=3 if attempt_type == "three" else 2,
                    )
                    if attempt_type != "three" and "block" in tpl and rng.random() < 0.10:
                        # ponytail: ~10% of two-point misses are blocked in the
                        # fixture feeds (v3 BLOCK rows / ESPN "Block Shot")
                        blocker = defense.sample_rebounder(rng)
                        _ensure_on_floor(not offense_is_home, blocker)
                        _emit(
                            "Block",
                            "",
                            tpl["block"].format(
                                name=_name(blocker),
                                blk=box.bump(box.blk, blocker),
                                label=_label(not offense_is_home),
                            ),
                            pid=blocker,
                        )
                elif token in ("oreb", "dreb"):
                    side_word = "Offensive" if token == "oreb" else "Defensive"
                    side_is_home = offense_is_home if token == "oreb" else not offense_is_home
                    if "team_rebound" in tpl and rng.random() < 0.06:
                        # ponytail: ~6% of fixture rebounds are team/deadball rows
                        _emit(
                            "Rebound",
                            side_word,
                            tpl["team_rebound"].format(label=team_labels[0] if side_is_home else team_labels[1]),
                        )
                    else:
                        side = offense if token == "oreb" else defense
                        rebounder = side.sample_rebounder(rng)
                        _ensure_on_floor(side_is_home, rebounder)
                        table = box.oreb if token == "oreb" else box.dreb
                        box.bump(table, rebounder)
                        _emit(
                            "Rebound",
                            side_word,
                            tpl["rebound"].format(
                                name=_name(rebounder),
                                side=side_word,
                                oreb=box.oreb.get(rebounder, 0),
                                dreb=box.dreb.get(rebounder, 0),
                                label=_label(side_is_home),
                            ),
                            pid=rebounder,
                        )
                elif token == "tov":
                    live = index + 1 < len(trail) and trail[index + 1] == "stl"
                    if not live and "team_turnover" in tpl and rng.random() < 0.05:
                        # ponytail: shot-clock team turnovers are ~5% of v3 turnovers
                        box.team_tov[offense_is_home] += 1
                        label = team_labels[0] if offense_is_home else team_labels[1]
                        _emit(
                            "Turnover",
                            "Shot Clock Turnover",
                            tpl["team_turnover"].format(label=label, team=box.team_tov[offense_is_home]),
                        )
                    else:
                        committer = offense.sample("tov", rng)
                        _ensure_on_floor(offense_is_home, committer)
                        personal = box.bump(box.tov, committer)
                        box.team_tov[offense_is_home] += 1
                        sub_type = "Lost Ball" if live else "Out of Bounds - Bad Pass Turnover"
                        _emit(
                            "Turnover",
                            sub_type,
                            tpl["turnover"].format(
                                name=_name(committer),
                                tov_subtype=sub_type,
                                personal=personal,
                                team=box.team_tov[offense_is_home],
                                label=_label(offense_is_home),
                            ),
                            pid=committer,
                        )
                        if live and "steal" in tpl:
                            stealer = defense.sample_rebounder(rng)
                            _ensure_on_floor(not offense_is_home, stealer)
                            _emit(
                                "Steal",
                                "Steal",
                                tpl["steal"].format(
                                    name=_name(stealer),
                                    stl=box.bump(box.stl, stealer),
                                    label=_label(not offense_is_home),
                                ),
                                pid=stealer,
                            )
                    index += 1  # consume the stl/tov_dead annotation
                elif token.startswith("ft_trip_"):
                    total = int(token.rsplit("_", 1)[1])
                    made = int(trail[index + 1].rsplit("_", 1)[1])
                    shooter = offense.sample(token, rng)
                    _ensure_on_floor(offense_is_home, shooter)
                    _foul(defense.sample_rebounder(rng), shooting=True)
                    _render_free_throws(shooter, made, total)
                    index += 1  # consume ft_made_k
                index += 1
            clock -= float(np.clip(game_rng.uniform(0.5, 1.5) * shelf.mean_possession_seconds, 4.0, 24.0))
            offense_is_home = not offense_is_home
        _period_row("end")
        if period >= rules.periods and score["home"] != score["away"]:
            break
        period += 1
        clock = rules.ot_seconds if period > rules.periods else rules.period_seconds
    if "game_end" in tpl:
        _emit("period", "game_end", tpl["game_end"])
    return {"score_home": score["home"], "score_away": score["away"]}, actions
