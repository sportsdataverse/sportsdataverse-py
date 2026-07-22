"""Provider play-text template registry — formulaic text, parameterized.

Every sim renderer assembles its play descriptions from THIS registry
instead of inline literals, so the formulaic dialect is a parameter:

* ``basketball`` — ``nba_stats`` (the stats.nba.com/stats.wnba.com
  ``playbyplayv3`` conventions used by NBA / WNBA / G-League), ``espn``
  (the MBB/WBB sentence style), and ``ncaa_stats`` (the stats.ncaa.org
  legacy pbp grammar);
* ``football`` — ``nfl_gsis`` (NFL.com GameCenter text) and ``espn``
  (the CFB sentence style);
* ``baseball`` — ``mlb_statsapi`` result descriptions;
* ``hockey`` — ``nhl_rtss`` report lines.

Templates are ``str.format`` strings over each event's context fields;
multi-variant events map to a tuple (the renderer samples one), and the
renderers pass a SUPERSET of fields so a template may use or ignore any
of them. Optional event keys are gated on PRESENCE — a provider without
``substitution``/``sub_in``+``sub_out``, ``jump_ball``,
``period_start``/``period_end``/``game_end``, ``steal``, ``block``,
``team_rebound``, ``team_turnover``, or the timeout variants simply never
emits those rows, which keeps each dialect faithful to what its real feed
publishes (e.g. ESPN college pbp has no substitution rows;
stats.ncaa.org has no period rows). A third-party dialect — a Fox- or
Yahoo-style house grammar — is just a :func:`register_provider` call over
the closest base; renderers pick it up by name immediately.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Tuple, Union

Template = Union[str, Tuple[str, ...]]
TemplateSet = Dict[str, Any]

_REGISTRY: Dict[str, Dict[str, TemplateSet]] = {
    "basketball": {
        "nba_stats": {
            "subtypes": {"rim": "Layup", "mid": "Jump Shot", "three": "3PT Jump Shot"},
            "made_shot": "{name} {distance}' {subtype} ({pts} PTS)",
            "assist_suffix": " ({assister} {ast} AST)",
            "missed_shot": "MISS {name} {distance}' {subtype}",
            "ft_make": "{name} Free Throw {attempt} of {total} ({pts} PTS)",
            "ft_miss": "MISS {name} Free Throw {attempt} of {total}",
            "rebound": "{name} REBOUND (Off:{oreb} Def:{dreb})",
            "turnover": "{name} {tov_subtype} Turnover (P{personal}.T{team})",
            "foul": "{name} P.FOUL (P{personal}.T{team})",
            "timeout": "{label} Timeout: Regular (Full {count} Short 0)",
            "steal": "{name} STEAL ({stl} STL)",
            "block": "{name} BLOCK ({blk} BLK)",
            "team_rebound": "{label} Rebound",
            "team_turnover": "{label} Turnover: Shot Clock (T#{team})",
            "foul_shooting": "{name} S.FOUL (P{personal}.T{team})",
            "official_suffix": " ({official})",
            "substitution": "SUB: {incoming} FOR {outgoing}",
            "jump_ball": "Jump Ball {center_home} vs. {center_away}: Tip to {tip_to}",
            "period_start": "Start of {ordinal} Period ({wall_time} EST)",
            "period_end": "End of {ordinal} Period ({wall_time} EST)",
            "violation": "{name} Violation: {kind} Violation",
            "instant_replay": "Instant Replay{ordinal} Period ({wall_time} EST)",
        },
        "espn": {
            "subtypes": {"rim": "Layup", "mid": "Jumper", "three": "Three Point Jumper"},
            "made_shot": "{name} made {subtype}.",
            "assist_suffix": " Assisted by {assister}.",
            "missed_shot": "{name} missed {subtype}.",
            "ft_make": "{name} made Free Throw.",
            "ft_miss": "{name} missed Free Throw.",
            "rebound": "{name} {side} Rebound.",
            "turnover": "{name} Turnover.",
            "steal": "{name} Steal.",
            "foul": "Foul on {name}.",
            "timeout": " Official TV Timeout",
            "timeout_team": "{label}  Timeout",
            "block": "{name} Block.",
            "team_rebound": "{label} Deadball Team Rebound.",
            "jump_ball": "Jump Ball won by {label}",
            "period_end": "End of {ordinal} {unit}",
            "game_end": "End of Game",
        },
        # the stats.ncaa.org legacy pbp grammar (assist and substitution
        # events are their own rows: "SMITH,JOHN DUKE Assist" /
        # "SMITH,JOHN DUKE Enters Game")
        "ncaa_stats": {
            "subtypes": {"rim": "Layup", "mid": "Two Point Jumper", "three": "Three Point Jumper"},
            "made_shot": "{name} {label} made {subtype}",
            "assist_row": "{name} {label} Assist",
            "missed_shot": "{name} {label} missed {subtype}",
            "ft_make": "{name} {label} made Free Throw",
            "ft_miss": "{name} {label} missed Free Throw",
            "rebound": "{name} {label} {side} Rebound",
            "team_rebound": "TEAM Deadball Rebound",
            "turnover": "{name} {label} Turnover",
            "team_turnover": "TEAM Turnover",
            "steal": "{name} {label} Steal",
            "block": "{name} {label} Blocked Shot",
            "foul": "{name} {label} Commits Foul",
            "timeout": "{label} Team Timeout",
            "timeout_media": "{label} Media Timeout",
            "sub_in": "{name} {label} Enters Game",
            "sub_out": "{name} {label} Leaves Game",
        },
    },
    "football": {
        "nfl_gsis": {
            "clock_prefix": "({minutes}:{seconds:02d}) ",
            "shotgun_prefix": "(Shotgun) ",
            "tackle_suffix": " ({tackler})",
            "rush_lanes": {"left": "left tackle", "middle": "up the middle", "right": "right end"},
            "rush": "{rusher} {lane} to {spot} for {yards} yards{tackle}.",
            "pass_complete": "{passer} pass {depth} {direction} to {receiver} to {spot} for {yards} yards{tackle}.",
            "pass_incomplete": "{passer} pass incomplete {depth} {direction} intended for {receiver}.",
            "sack": "{passer} sacked at {spot} for {yards} yards{tackle}.",
            "interception": "{passer} pass {depth} {direction} intended for {receiver} INTERCEPTED by {defender} at {spot}.",
            "fumble_lost": "{rusher} FUMBLES, recovered by {defense_abbr}.",
            "punt": "{punter} punts {yards} yards to {spot}.",
            "fg_good": "{kicker} {kick_distance} yard field goal is GOOD.",
            "fg_miss": "{kicker} {kick_distance} yard field goal is No Good.",
            "touchdown": "{offense_abbr} TOUCHDOWN ({xp}).",
            "xp_good": "kick is good",
            "xp_fail": "kick failed",
            "kickoff_touchback": "{kicker} kicks {kick_yards} yards from {kick_from} to end zone, Touchback to the {recv_spot}.",
            "kickoff_return": "{kicker} kicks {kick_yards} yards from {kick_from} to {land_spot}. {returner} to {recv_spot} for {ret_yards} yards ({tackler}).",
            "end_period": "END QUARTER {quarter}",
            "end_game": "END GAME",
            "two_minute_warning": "Two-Minute Warning",
            "timeout_team": "Timeout #{count} by {abbr} at {clock}.",
            "timeout_official": "Official Timeout at {clock}.",
            "penalty": "PENALTY on {abbr}-{player}, {infraction}, {pen_yards} yards, enforced at {spot} - No Play.",
        },
        "espn": {
            "rush_gain": "{rusher} run for {yards} yds to the {spot}{first_down}",
            "rush_loss": "{rusher} run for a loss of {loss} yard{plural} to the {spot}",
            "first_down_suffix": " for a 1ST down",
            "pass_complete": "{passer} pass complete to {receiver} for {yards} yds to the {spot}{first_down}",
            "pass_incomplete": "{passer} pass incomplete to {receiver}",
            "sack": "{passer} sacked by {defender} for a loss of {loss} yards to the {spot}",
            "interception": "{passer} pass intercepted {defender} return for no gain to the {spot}",
            "fumble_lost": "{rusher} run for {yards} yds, fumbled, recovered by {defense_abbr}",
            "punt": "{punter} punt for {yards} yds",
            "fg_good": "{kicker} {kick_distance} yd FG GOOD",
            "fg_miss": "{kicker} {kick_distance} yd FG MISSED",
            "touchdown": "{offense_abbr} TOUCHDOWN, {xp}",
            "xp_good": "kick good",
            "xp_fail": "kick failed",
            "kickoff_touchback": "{kicker} kickoff for {kick_yards} yds for a touchback",
            "kickoff_return": "{kicker} kickoff for {kick_yards} yds , {returner} return for {ret_yards} yds to the {recv_spot}",
            "end_period": "End of {ordinal} Quarter",
            "end_game": "End of {ordinal} Quarter",
            "timeout_team": "Timeout {team}, clock {clock}",
            "penalty": "PENALTY {abbr} {infraction} ({player}) {pen_yards} yards to the {spot}, NO PLAY.",
        },
    },
    "baseball": {
        "mlb_statsapi": {
            "so": (
                "{batter} strikes out swinging.",
                "{batter} called out on strikes.",
            ),
            "bb": "{batter} walks.",
            "single": "{batter} singles on a sharp line drive to center field.",
            "double": "{batter} doubles on a line drive to left field.",
            "triple": "{batter} triples on a fly ball to right field.",
            "hr": "{batter} homers ({count}) on a fly ball to left center field.",
            "out_inplay": (
                "{batter} grounds out, shortstop to first baseman.",
                "{batter} flies out to center fielder.",
            ),
            "gidp": "{batter} grounds into a double play, second baseman to shortstop to first baseman.",
            "sac_fly": "{batter} out on a sacrifice fly to center field.",
            "reach_other": "{batter} reaches on a fielding error.",
            "other_out": "{runner} caught stealing 2nd base.",
            "advance_suffix": " {runner} to {base}.",
            "score_suffix": " {runner} scores.",
            "pitching_sub": "Pitching Change: {incoming} replaces {outgoing}.",
            "offensive_sub": "Offensive Substitution: Pinch-hitter {incoming} replaces {outgoing}.",
            "mound_visit": "Mound Visit.",
            "batter_timeout": "Batter Timeout.",
            "game_advisory": "Status Change - {status}",
        },
    },
    "hockey": {
        "nhl_rtss": {
            "shot_on_goal": "{abbr} ONGOAL - {player}, Wrist, Off. Zone, {distance} ft.",
            "goal": "{abbr} {player}({count}), Wrist, Off. Zone, {distance} ft.",
            "missed_shot": "{abbr} {player}, Wrist, Wide of Net, Off. Zone, {distance} ft.",
            "blocked_shot": "{abbr} {player} BLOCKED BY {opp_abbr} {blocker}, Wrist, Def. Zone.",
            "hit": "{abbr} {player} HIT {opp_abbr} {victim}, Def. Zone.",
            "faceoff": "{abbr} won {zone}. Zone - {away_abbr} {away_player} vs {home_abbr} {home_player}",
            "giveaway": "{abbr} GIVEAWAY - {player}, Def. Zone.",
            "takeaway": "{abbr} TAKEAWAY - {player}, Neu. Zone.",
            "penalty": "{abbr} {player}, {infraction} (2 min)",
            "shootout": "{abbr} wins the shootout",
            "stoppage": "{reason}",
            "assists_suffix": " Assists: {assists}",
            "assist_item": "{player}({count})",
            "delayed_penalty": "Delayed Penalty - {abbr}",
            "period_start": "Period Start- Local time: {wall_time} EDT",
            "period_end": "Period End- Local time: {wall_time} EDT",
            "game_end": "Game End- Local time: {wall_time} EDT",
        },
    },
}

#: Renderer-facing aliases for the historical dialect names.
PROVIDER_ALIASES = {
    "v3": "nba_stats",
    "ncaa": "ncaa_stats",
    "stats_ncaa": "ncaa_stats",
    "espn_college": "espn",
    "gsis": "nfl_gsis",
    "college": "espn",
    "statsapi": "mlb_statsapi",
    "rtss": "nhl_rtss",
}


def get_templates(family: str, provider: str) -> TemplateSet:
    """The template set for a (family, provider) pair.

    Args:
        family: ``basketball`` / ``football`` / ``baseball`` / ``hockey``.
        provider: Registered provider name (historical dialect aliases
            like ``v3`` / ``gsis`` / ``college`` resolve automatically).

    Returns:
        The template dict (a deep copy — mutate freely).

    Raises:
        KeyError: On an unknown family or provider.

    Example:
        Quick start::

            from sportsdataverse._common.play_text import get_templates
            tpl = get_templates("basketball", "nba_stats")
            tpl["made_shot"].format(name="Turner", distance=2, subtype="Layup", pts=12)
    """
    providers = _REGISTRY[family]
    resolved = PROVIDER_ALIASES.get(provider, provider)
    if resolved not in providers:
        raise KeyError(f"Unknown provider {provider!r} for {family!r}; registered: {sorted(providers)}")
    return copy.deepcopy(providers[resolved])


def register_provider(
    family: str,
    provider: str,
    templates: TemplateSet,
    *,
    base: Optional[str] = None,
) -> None:
    """Register (or extend) a provider's formulaic templates.

    Args:
        family: Sport family the templates belong to.
        provider: New provider name.
        templates: Template entries; with ``base`` given, these OVERRIDE the
            base provider's entries (single-formula overrides are fine).
        base: Optional existing provider to inherit from.

    Raises:
        KeyError: On an unknown family or base provider.

    Example:
        A Fox/Yahoo-style dialect is just a registration — inherit the
        closest base and override the formulas that differ::

            from sportsdataverse._common.play_text import register_provider
            register_provider(
                "basketball", "foxsports",
                {"made_shot": "{name} makes {subtype}", "foul": "Personal foul on {name}"},
                base="espn",
            )
            # then render with simulate_game_actions(..., provider="foxsports")
    """
    providers = _REGISTRY[family]
    merged: TemplateSet = get_templates(family, base) if base is not None else {}
    merged.update(copy.deepcopy(templates))
    providers[provider] = merged


def registered_providers(family: str) -> "list[str]":
    """The provider names registered for a family.

    Args:
        family: Sport family.

    Returns:
        Sorted provider names.
    """
    return sorted(_REGISTRY[family])
