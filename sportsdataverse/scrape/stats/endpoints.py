"""Declarative capture registry for the stats.{nba,wnba}.com surface.

One module drives both leagues and both raw repos; only ``league_id`` differs.

Rather than hand-maintaining ~90 endpoint entries and their parameter matrices,
the matrix for each endpoint is **derived from its own signature**: an endpoint
that accepts a ``season_type*`` parameter gets swept over season types, one that
accepts ``measure_type*`` over measure types, and so on. A new endpoint appearing
upstream is therefore captured at the right granularity with no edit here, and an
endpoint that drops a parameter stops being swept over it instead of 400-ing.

Granularity choices, made for reuse rather than for any one current consumer:

* **Game endpoints** are captured whole-game, one payload per game per endpoint.
  Anything narrower (period, range) is a strict subset that can be re-requested,
  and the per-period capture already exists separately for lineup grounding.
* **Season endpoints** are captured at ``Totals`` *and* ``PerGame``. Totals is the
  information-dense form -- PerGame, Per36 and Per100 are all derivable from it --
  but the currently published datasets are PerGame, and deriving them would
  introduce rounding differences against what consumers already read. Season-level
  calls are cheap enough (tens per season, against thousands per season of games)
  that capturing both removes the question entirely.
* Every call pins ``season`` and ``league_id`` explicitly rather than relying on
  upstream defaults, which are undocumented and free to drift.

Capturing a superset is deliberate: a payload already on disk costs nothing to
reshape later, while a payload never captured means re-sweeping a decade.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from typing import Any, Callable

LEAGUE_NBA = "00"
LEAGUE_WNBA = "10"

SEASON_TYPES = ("Regular Season", "Playoffs")
#: Every measure type the API defines. "Four Factors" was missing from this
#: tuple, so it was never captured for ANY endpoint -- a whole measure type
#: absent from the archive rather than merely mis-parameterised.
MEASURE_TYPES = (
    "Base",
    "Advanced",
    "Misc",
    "Four Factors",
    "Scoring",
    "Usage",
    "Defense",
    "Opponent",
)

#: Measure-type values each parameter actually accepts, keyed by the parameter's
#: OWN name. Sweeping all of MEASURE_TYPES over every ``measure_type*`` parameter
#: is what produced most of this archive's empty payloads: the endpoint accepts
#: the parameter, but the API answers an unsupported value with a body that does
#: not parse, and that `{}` was persisted and never retried.
#:
#: Measured live (2026-08-01), not taken from documentation:
#:   measure_type_simple            Base/Opponent only -> the other 6 were 5/7
#:                                  of every shot-locations capture (71.4% empty,
#:                                  identical in NBA and WNBA)
#:   measure_type_detailed_defense  everything except Usage -> 1/7 (14.3% empty,
#:                                  again identical across both leagues)
#: An unlisted parameter falls back to the full tuple.
MEASURE_TYPE_DOMAINS: dict[str, tuple[str, ...]] = {
    "measure_type_simple": ("Base", "Opponent"),
    "measure_type_detailed_defense": MEASURE_TYPES,
    # The PLAYER-side domain. Four Factors / Opponent answer {} on
    # playergamelogs (calm re-probe 2026-08-02) -- they are team-level
    # concepts, exactly mirroring Usage being player-only. teamgamelogs
    # shares this parameter NAME but not this domain; its override below
    # carries Four Factors + Opponent instead of Usage.
    "measure_type_player_game_logs_nullable": (
        "Base",
        "Advanced",
        "Misc",
        "Scoring",
        "Usage",
    ),
}

#: Per-ENDPOINT narrowing, applied on top of the parameter default above.
#:
#: The domain is not purely a property of the parameter. leaguedashteamstats and
#: leaguedashplayerstats both take `measure_type_detailed_defense`, but only the
#: team one rejects Usage -- so keying solely by parameter name silently dropped
#: Usage from leaguedashlineups / leaguedashplayerclutch / leaguedashplayerstats
#: / leaguedashteamclutch / leaguelineupviz, all of which do support it.
#:
#: Derived by scanning the committed archive: a measure that is empty in EVERY
#: captured season is unsupported; one populated in any season is supported.
#: That is a far larger and more stable sample than live probing, which throttles
#: and returns inconsistent negatives.
ENDPOINT_MEASURE_TYPES: dict[str, tuple[str, ...]] = {
    "leaguedashteamstats": tuple(m for m in MEASURE_TYPES if m != "Usage"),
    # Measured live 2026-08-02 (both leagues): Four Factors and Opponent each
    # return full seasons (NBA 2,460 rows, WNBA 480), so the earlier
    # Base/Advanced/Misc/Scoring tuple -- derived by scanning an archive
    # captured under the poisoned TeamID default -- was two measures short.
    # Usage stays out: {} on teamgamelogs in both leagues even with valid
    # params (usage is a player concept; the team FF/Opponent measures are
    # the mirror image, {} on playergamelogs).
    "teamgamelogs": ("Base", "Advanced", "Four Factors", "Misc", "Scoring", "Opponent"),
}


def measure_types_for(fn_name: str, param: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Values to sweep for ``param`` on the endpoint behind ``fn_name``.

    Endpoint override beats parameter default beats the caller's full list.

    ``fn_name`` is the wrapper's full name (``nba_stats_leaguedashteamstats``),
    matched by SUFFIX. Splitting on the first underscore would be wrong -- the
    league prefix itself contains one -- and this function has no access to the
    prefix ``discover()`` used.
    """
    # Guard the axis. _SWEEPS also carries season_type and per_mode, and an
    # endpoint override applied to those would set season_type_all_star="Base"
    # and per_mode_detailed="Misc" -- silently turning one endpoint's matrix
    # into the cube of its measure types.
    if not param.startswith("measure_type"):
        return default
    for endpoint, values in ENDPOINT_MEASURE_TYPES.items():
        if fn_name == endpoint or fn_name.endswith(f"_{endpoint}"):
            return values
    return MEASURE_TYPE_DOMAINS.get(param, default)


#: Season / league parameters, most-specific first. Matched by EXACT name from
#: this list rather than by prefix: prefix-matching "season" would also hit
#: ``season_segment_nullable`` and ``season_type_*``. The previous code tested
#: only the bare ``season``, so endpoints that spell it ``season_nullable``
#: (playergamelogs, teamgamelogs) were called with NO season filter at all --
#: the API answered nothing and 100% of those captures were empty in both
#: leagues.
#:
#: ``season_year_nullable`` is the same bug one spelling further out, and it
#: fails LOUDER than an empty capture: drafthistory is the only endpoint that
#: spells it that way, and unfiltered it answers with the FULL draft history
#: (1947-2026) rather than nothing. A season sweep that drops the filter writes
#: that same payload under every season -- which is exactly the state
#: wehoop-wnba-stats-raw is in (30 byte-identical drafthistory/{season}.json,
#: all md5 b682aa93cc, "Season": null in each one's echoed parameters).
_SEASON_PARAMS = (
    "season",
    "season_nullable",
    "season_year",
    "season_year_nullable",
    "season_all_time",
)
_LEAGUE_PARAMS = ("league_id", "league_id_nullable")

PER_MODES = ("Totals", "PerGame")

#: Sub-dimension axes. These endpoints take a REQUIRED extra axis; before
#: 2026-08-02 the sweep left each at its wrapper default, so the archive held
#: one slice of a much larger surface: synergyplaytypes = Isolation/Offensive
#: only (1 of 22), leaguedashptstats = Drives only (1 of 12), leaguedashptdefend
#: = Overall only (1 of 6). Values from nba_api parameters.py (PtMeasureType /
#: DefenseCategory / PlayType classes) -- the API's own domain model.
PT_MEASURE_TYPES = (
    "SpeedDistance",
    "Rebounding",
    "Possessions",
    "CatchShoot",
    "PullUpShot",
    "Defense",
    "Drives",
    "Passing",
    "ElbowTouch",
    "PostTouch",
    "PaintTouch",
    "Efficiency",
)
DEFENSE_CATEGORIES = (
    "Overall",
    "3 Pointers",
    "2 Pointers",
    "Less Than 6Ft",
    "Less Than 10Ft",
    "Greater Than 15Ft",
)
#: Synergy spellings are the API's own: PRBallHandler / PRRollman, and
#: putbacks are "OffRebound".
PLAY_TYPES = (
    "Transition",
    "Isolation",
    "PRBallHandler",
    "PRRollman",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
)
TYPE_GROUPINGS = ("Offensive", "Defensive")

#: Season-level endpoints that must never be swept per-season.
#: scoreboardv3 is DATE-keyed (GameDate=YYYY-MM-DD); sweeping it per season
#: captured the wrapper's fixed default date over and over -- one junk file per
#: season. Its content (the day's games) is fully covered by the per-game
#: endpoints. Excluded in discover(), the single registry gate.
#: shotchartlineupdetail is LINEUP-keyed: it requires a GroupID (a 5-man
#: lineup id) and the roxygen-mined default pinned one specific lineup, so
#: every "season" capture was one lineup's shots. A season sweep cannot
#: enumerate lineups; per-lineup capture is an entity-iteration design.
EXCLUDED_SEASON_ENDPOINTS = frozenset({"scoreboardv3", "shotchartlineupdetail"})

#: Lineups are five-player units; the endpoint also accepts 2-4 but the published
#: datasets are 5-man and the smaller units are a much larger combinatorial space.
LINEUP_GROUP_QUANTITY = 5

#: Parameter-name prefix -> the values to sweep it over. Prefix-matched because the
#: same concept is spelled differently per endpoint (``season_type_all_star``,
#: ``season_type_playoffs``, ``season_type_nullable``).
_SWEEPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("season_type", SEASON_TYPES),
    ("measure_type", MEASURE_TYPES),
    ("pt_measure_type", PT_MEASURE_TYPES),
    ("defense_category", DEFENSE_CATEGORIES),
    ("play_type", PLAY_TYPES),
    ("type_grouping", TYPE_GROUPINGS),
    ("per_mode", PER_MODES),
)

#: Parameters pinned to a single value when the endpoint accepts them.
_PINS: tuple[tuple[str, Any], ...] = (("group_quantity", LINEUP_GROUP_QUANTITY),)

#: Season parameters that want the NBA's two-year span string ("2023-24").
#: NBA ONLY: the WNBA plays inside one calendar year and its API takes the
#: bare year (measured on the WNBA sweep; a span string is not the WNBA's
#: spelling). The span decision is keyed off ``league_id`` in
#: :func:`season_variants`.
#:
#: `season_year` is deliberately absent: on the draftcombine* endpoints the
#: probed shape is a bare draft year ("2019" -> 77 rows, 2026-08-02; the span
#: is ALSO accepted, but bare is what hoopR sends and what the archive holds).
#: `season_all_time` (draftcombinestats' spelling) IS spanned -- "2019-20"
#: returned 77 rows on the same probe; the sweep had never sent it a season at
#: all because the name wasn't in _SEASON_PARAMS, so the endpoint was
#: misdiagnosed as parameter-broken.
_SPAN_SEASON_PARAMS = ("season", "season_nullable", "season_all_time")


def season_string(season: int) -> str:
    """The NBA's own spelling of a season: 2023 -> "2023-24", 1999 -> "1999-00"."""
    return f"{season}-{str(season + 1)[-2:]}"


def _params(fn: Callable[..., Any]) -> set[str]:
    try:
        return set(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        return set()


def _match(params: set[str], prefix: str) -> str | None:
    """The endpoint's own spelling of a swept parameter, if it accepts one."""
    for name in sorted(params):
        if name.startswith(prefix):
            return name
    return None


def slug(value: Any) -> str:
    """Filename-safe parameter value (``Regular Season`` -> ``regular-season``)."""
    return str(value).lower().replace(" ", "-").replace("_", "-")


def discover(module: Any, prefix: str) -> tuple[list[str], list[str]]:
    """``(game_endpoints, season_endpoints)`` exposed by a league's stats module.

    Team- and player-keyed endpoints are excluded: they are addressed by an id this
    sweep does not enumerate, and are a separate (much larger) capture decision.
    """
    game: list[str] = []
    season: list[str] = []
    for name in sorted(dir(module)):
        if not name.startswith(f"{prefix}_"):
            continue
        fn = getattr(module, name)
        if not callable(fn):
            continue
        params = _params(fn)
        if not params:
            continue
        short = name[len(prefix) + 1 :]
        if short in EXCLUDED_SEASON_ENDPOINTS:
            continue
        if "game_id" in params:
            game.append(short)
        elif "team_id" in params or "player_id" in params:
            continue
        else:
            season.append(short)
    return game, season


def season_variants(fn: Callable[..., Any], season: int, league_id: str) -> Iterator[tuple[str | None, dict[str, Any]]]:
    """Yield ``(variant_slug, kwargs)`` for every capture of one season endpoint.

    The slug is built only from the parameters this endpoint actually sweeps, so
    an endpoint gaining an unrelated parameter later cannot rename existing
    captures, and two endpoints never collide on a filename.
    """
    params = _params(fn)
    base: dict[str, Any] = {}
    season_param = next((p for p in _SEASON_PARAMS if p in params), None)
    if season_param:
        # A BARE year silently returns zero rows on several endpoints while
        # others tolerate it, so the sweep looked healthy while
        # leagueleaders / leaguedashptstats / leaguedashptdefend /
        # leaguedashteamptshot / leaguedashplayerptshot / leaguedashoppptshot
        # captured a valid envelope with no data, every season, for years.
        #
        # Measured: leagueleaders 0 -> 240 rows, leaguedashptstats 0 -> 572,
        # leaguedashptdefend 0 -> 569 once the span string is sent.
        #
        # Tolerant endpoints are unaffected: "2023" and "2023-24" return
        # byte-identical rows (checked against leaguedashteamstats), so the
        # API already reads a bare year as the START year. This only makes
        # that explicit.
        base[season_param] = (
            season_string(season) if league_id == LEAGUE_NBA and season_param in _SPAN_SEASON_PARAMS else str(season)
        )
    league_param = next((p for p in _LEAGUE_PARAMS if p in params), None)
    if league_param:
        base[league_param] = league_id
    for pin, value in _PINS:
        name = _match(params, pin)
        if name:
            base[name] = value

    # Expand the cartesian product of whichever sweeps this endpoint supports.
    # Each axis is narrowed to the values its OWN parameter accepts, so the
    # sweep stops issuing calls the API cannot answer.
    axes: list[tuple[str, tuple[str, ...]]] = []
    for prefix, values in _SWEEPS:
        name = _match(params, prefix)
        if name:
            axes.append((name, measure_types_for(fn.__name__, name, values)))

    if not axes:
        yield None, base
        return

    def walk(i: int, acc: dict[str, Any], parts: list[str]) -> Iterator[tuple[str, dict[str, Any]]]:
        if i == len(axes):
            yield "_".join(parts), {**base, **acc}
            return
        name, values = axes[i]
        for value in values:
            yield from walk(i + 1, {**acc, name: value}, [*parts, slug(value)])

    yield from walk(0, {}, [])


def plan_counts(module: Any, prefix: str, league_id: str, season: int = 2025) -> dict[str, int]:
    """Per-season call counts, for sizing a sweep before running one."""
    game, season_eps = discover(module, prefix)
    n_season = sum(
        len(list(season_variants(getattr(module, f"{prefix}_{ep}"), season, league_id))) for ep in season_eps
    )
    return {
        "game_endpoints": len(game),
        "season_endpoints": len(season_eps),
        "season_calls_per_season": n_season,
    }
