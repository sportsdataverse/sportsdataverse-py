"""League identity for the ESPN ``-raw`` scrape engine.

One frozen config per league carries the facts the engine cannot derive: the
tree prefix the archive hangs off, the owning ``-raw`` repository (which is
also the host of the public ``raw.githubusercontent`` URLs the schedules
advertise), and which per-game payload families that league actually has.

The families differ for real reasons, not oversight -- ESPN publishes an
officials feed for the two women's leagues and not for the two men's, and only
the two professional leagues have a draft. Encoding that here means a league
without a family simply never gets its column, instead of every scraper
carrying a conditional.
"""

from __future__ import annotations

from dataclasses import dataclass

#: ``(column stem, tree segments under <league>/)`` for a per-game payload
#: family. The stem names both the ``<stem>_url`` column and, when flagged,
#: the ``has_<stem>`` capture boolean.
Family = tuple[str, tuple[str, ...]]

_GAME_JSON: Family = ("game_json", ("json", "final"))
_GAME_JSON_RAW: Family = ("game_json_raw", ("json", "raw"))
_GAME_ROSTERS: Family = ("game_rosters_json", ("game_rosters", "json"))
_OFFICIALS: Family = ("officials_json", ("officials", "json"))


@dataclass(frozen=True)
class LeagueConfig:
    """Identity facts for one ESPN ``-raw`` archive.

    Attributes:
        key: Tree prefix and short league slug (``"nba"`` / ``"mbb"`` /
            ``"wnba"`` / ``"wbb"``). Every path under the archive hangs off it.
        repo: Owning ``-raw`` repository, used to build the public
            ``raw.githubusercontent`` URLs the season schedules carry.
        families: Per-game payload families this league publishes.
        flagged: Stems that get a ``has_<stem>`` capture boolean.
        logfile: The repo's module-level log destination.

    Note:
        ``game_json_raw`` is deliberately absent from ``flagged`` everywhere:
        it is written by the same call as ``game_json``, so their presence
        cannot diverge and a second flag could only ever lie.
    """

    key: str
    repo: str
    families: tuple[Family, ...]
    flagged: tuple[str, ...]
    logfile: str


NBA = LeagueConfig(
    key="nba",
    repo="hoopR-nba-raw",
    families=(_GAME_JSON, _GAME_JSON_RAW, _GAME_ROSTERS),
    flagged=("game_json", "game_rosters_json"),
    logfile="hoopR_nba_raw_logfile.txt",
)

MBB = LeagueConfig(
    key="mbb",
    repo="hoopR-mbb-raw",
    families=(_GAME_JSON, _GAME_JSON_RAW, _GAME_ROSTERS),
    flagged=("game_json", "game_rosters_json"),
    logfile="hoopR_mbb_raw_logfile.txt",
)

WNBA = LeagueConfig(
    key="wnba",
    repo="wehoop-wnba-raw",
    families=(_GAME_JSON, _GAME_JSON_RAW, _GAME_ROSTERS, _OFFICIALS),
    flagged=("game_json", "game_rosters_json", "officials_json"),
    logfile="wehoop_wnba_raw_logfile.txt",
)

WBB = LeagueConfig(
    key="wbb",
    repo="wehoop-wbb-raw",
    families=(_GAME_JSON, _GAME_JSON_RAW, _GAME_ROSTERS, _OFFICIALS),
    flagged=("game_json", "game_rosters_json", "officials_json"),
    logfile="wehoop_wbb_raw_logfile.txt",
)

_BY_KEY: dict[str, LeagueConfig] = {c.key: c for c in (NBA, MBB, WNBA, WBB)}


def by_key(key: str) -> LeagueConfig:
    """Look up a league config by its slug.

    Args:
        key: ``"nba"``, ``"mbb"``, ``"wnba"``, or ``"wbb"``.

    Returns:
        The frozen config for that league.

    Raises:
        ValueError: If the slug is unknown. Naming the valid set in the message
            matters here because the caller is usually a shell driver passing
            ``--league`` straight through from a cron definition.
    """
    try:
        return _BY_KEY[key]
    except KeyError:
        valid = ", ".join(sorted(_BY_KEY))
        raise ValueError(f"unknown ESPN league {key!r}; expected one of: {valid}") from None
