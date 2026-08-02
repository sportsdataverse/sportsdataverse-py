"""League identity for the stats.nba.com / stats.wnba.com sweep engine.

One frozen config per league carries the identity facts the engine cannot
derive: which wrapper module serves the league, the ``LeagueID`` the API
expects, and where the owning ``-raw`` repo keeps its store. Everything
behavioral (season-string spelling, period time math) lives in
:mod:`~sportsdataverse.scrape.stats.endpoints` and
:mod:`~sportsdataverse.scrape.stats.periods`, keyed off ``league_id`` — the
same key the capture planners already thread through every call.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LeagueConfig:
    """Identity facts for one league's raw-store sweep.

    Attributes:
        key: Short league slug (``"nba"`` / ``"wnba"``).
        league_id: The stats API's ``LeagueID`` (``"00"`` NBA, ``"10"`` WNBA).
        stats_module: Import path of the sdv-py wrapper module whose
            ``{stats_prefix}_<endpoint>`` callables the sweep drives.
        stats_prefix: Wrapper-name prefix (``nba_stats`` / ``wnba_stats``).
        repo: Owning ``-raw`` repository name (log/error messages only).
        store_env: Environment variable that overrides the raw-store root.
        store_subdir: Store path components under the owning repo's root.
    """

    key: str
    league_id: str
    stats_module: str
    stats_prefix: str
    repo: str
    store_env: str
    store_subdir: tuple[str, ...]


NBA = LeagueConfig(
    key="nba",
    league_id="00",
    stats_module="sportsdataverse.nba.nba_stats",
    stats_prefix="nba_stats",
    repo="hoopR-nba-stats-raw",
    store_env="SDV_PY_NBA_RAW_JSON_DIR",
    store_subdir=("nba_stats", "json"),
)

WNBA = LeagueConfig(
    key="wnba",
    league_id="10",
    # wehoop-wnba-stats-raw's pre-migration refill_empty.py imported the
    # nonexistent `sportsdataverse.nba.wnba_stats` — a latent crash on its
    # non-check path that the config test here caught. This is the real module.
    stats_module="sportsdataverse.wnba.wnba_stats",
    stats_prefix="wnba_stats",
    repo="wehoop-wnba-stats-raw",
    store_env="SDV_PY_WNBA_RAW_JSON_DIR",
    store_subdir=("wnba_stats", "json"),
)

_BY_LEAGUE_ID = {c.league_id: c for c in (NBA, WNBA)}


def by_league_id(league_id: str) -> LeagueConfig:
    """The config for a stats ``LeagueID`` (raises ``KeyError`` on unknown)."""
    return _BY_LEAGUE_ID[league_id]
