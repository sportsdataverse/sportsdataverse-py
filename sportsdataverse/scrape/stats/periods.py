"""Per-period boxscore windows — league- and era-aware time math.

``boxscoretraditionalv3`` accepts a ``RangeType=2`` request window; asking for
the one-second window at a period's opening tick returns exactly the players on
court when that period began. Those captures let a downstream builder
reconstruct on-court lineups from period starts + substitutions instead of
inferring them from play-by-play alone.

**The window is league- AND season-specific**, which is why every function here
takes an explicit ``league_id`` (and refuses to default the season):

* NBA (``"00"``): four 12-minute quarters for the whole span the stores cover.
* WNBA (``"10"``): two 20-minute halves through 2005, four 10-minute quarters
  from 2006 (confirmed from captured play-by-play: every season 1997-2005 has a
  modal max period of 2 opening at ``PT20M00.00S``; 2006 onward is 4 periods at
  ``PT10M00.00S``). Regulation is 2400s in both eras, so a regulation-total
  check cannot catch a mix-up — the *period boundaries* differ by ten minutes,
  and a wrong window returns a well-formed boxscore for the wrong moment rather
  than an error.

Overtime is 5 minutes in both leagues and both eras.

Game-id season decoding also differs: an NBA season spans two calendar years,
so the store's directory is the id's start year **plus one**; the WNBA plays
inside one calendar year.

A regression test pins the NBA math to
``sportsdataverse.nba.nba_lineups._period_start_range`` (the same function the
possession engine uses when it reads these payloads back), so the capture
window cannot drift from what the reader expects.
"""

#: ``RangeType=2`` selects an explicit Start/EndRange window (pbpstats convention).
QUARTER_BOX_RANGE_TYPE = "2"

#: One-second opening window, in tenths — same width sdv-py and pbpstats use.
WINDOW_WIDTH_TENTHS = 10

#: Guard against a malformed payload driving an unbounded fetch loop.
MAX_PERIODS = 12

#: Overtime is 5 minutes in both leagues and both eras.
OT_PERIOD_SECONDS = 300

#: ``league_id -> ((from_season, periods, seconds_per_period), ...)`` newest era
#: first. The first entry whose ``from_season`` the season reaches wins.
_REGULATION_ERAS: dict[str, tuple[tuple[int, int, int], ...]] = {
    "00": ((0, 4, 720),),
    "10": ((2006, 4, 600), (0, 2, 1200)),
}

#: Season-directory year relative to the game id's two-digit start year: an NBA
#: season spans two calendar years (store keys the END year); a WNBA season is
#: one calendar year.
_GAME_ID_YEAR_OFFSET = {"00": 1, "10": 0}


def regulation_shape(season: int, league_id: str) -> tuple[int, int]:
    """``(periods, seconds_per_period)`` for ``season``'s regulation format."""
    for from_season, periods, secs in _REGULATION_ERAS[league_id]:
        if season >= from_season:
            return periods, secs
    raise ValueError(f"no regulation era covers season {season}")


def period_elapsed_seconds(period: int, season: int, league_id: str) -> int:
    """Game seconds elapsed when ``period`` (1-indexed) opens in ``season``.

    ``season`` is required rather than defaulted: the WNBA halves/quarters split
    makes a silent default wrong for a third of that league's history.
    """
    periods, per_period = regulation_shape(season, league_id)
    if period <= periods:
        return (period - 1) * per_period
    return periods * per_period + (period - periods - 1) * OT_PERIOD_SECONDS


def period_start_range(period: int, season: int, league_id: str) -> tuple[str, str]:
    """``(StartRange, EndRange)`` in tenths of a second at ``period``'s opening tick."""
    start = period_elapsed_seconds(period, season, league_id) * 10
    return str(start), str(start + WINDOW_WIDTH_TENTHS)


def periods_in_game(pbp_payload: object) -> int:
    """Highest period number in a captured ``playbyplayv3`` payload (0 if unknown).

    Reading it off the stored play-by-play means overtime is discovered for
    free — a fixed regulation-period fetch would truncate every OT game, and
    probing for it would spend a request per game.
    """
    if not isinstance(pbp_payload, dict):
        return 0
    actions = (pbp_payload.get("game") or {}).get("actions") or []
    best = 0
    for action in actions:
        if isinstance(action, dict):
            try:
                best = max(best, int(action.get("period") or 0))
            except (TypeError, ValueError):
                continue
    return min(best, MAX_PERIODS)


def season_of(game_id: str, league_id: str) -> int:
    """Season directory year encoded in a 10-digit stats game id.

    Digits 3-4 are the season's two-digit start year (``>= 90`` is 19xx). NBA
    ``0020500469`` -> 2006 (season spans two years; the store keys the END
    year); WNBA ``1022600071`` -> 2026 (single calendar year).
    """
    gid = str(game_id).zfill(10)
    yy = int(gid[3:5])
    start = 1900 + yy if yy >= 90 else 2000 + yy
    return start + _GAME_ID_YEAR_OFFSET[league_id]
