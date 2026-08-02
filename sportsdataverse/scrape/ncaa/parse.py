"""Decoupled PARSE step: raw captured bundle -> combined parsed JSON.

Reads a raw bundle written by :mod:`ncaa_bundle` (``play_by_play`` /
``box_score`` / ``individual_stats`` pages) and runs it through the sdv-py
bigballR/cbb-explorer parser stack, producing ONE combined record with six
datasets: ``pbp``, ``lineups``, ``player_box``, ``team_box``, ``shots``,
``possessions``.

**Robustness contract:** each of the six families is computed in its own
``try/except`` -- a parser bug in one family (e.g. a legacy box-score layout
tripping ``get_box_lineup``) logs a warning and yields ``[]`` for that family
only; it never aborts the other five. Across ~6k real games some games WILL
hit parser edge cases -- a game with 5/6 families populated is a success, a
crashed run is not.

Call sequence per family (see ``tests/mbb/test_mbb_ncaa_lineup_aggregation_e2e.py``
for the ``get_box_lineup`` -> ``create_lineup_data`` sequence this mirrors):

* ``pbp`` -- ``parse_ncaa_bb_game_pbp(play_by_play_html, contest_id)``.
* ``lineups`` -- per team (home, away): ``get_box_lineup(individual_stats_html,
  TeamId(team))`` then ``create_lineup_data(play_by_play_html, box_lineup)``;
  the "good" stints from both teams are concatenated.
* ``player_box`` / ``team_box`` -- ``ncaa_mbb_player_stats(pbp)`` /
  ``ncaa_mbb_team_stats(pbp)``, aggregated directly off the parsed pbp frame
  (no extra HTML parse needed -- the box-score page carries no data these two
  don't already derive from play-by-play).
* ``shots`` -- ``get_box_lineup(individual_stats_html, TeamId(home))`` (fresh
  call, kept independent of the ``lineups`` family) feeds
  ``create_shot_event_data(box_score_html, box_lineup)`` (the SVG shot map
  lives on the ``box_score`` page, confirmed against the committed fixtures --
  NOT the ``individual_stats`` page despite the module docstring's generic
  "box-score-page" wording), then ``shot_events_to_frame(...)``.
* ``possessions`` -- ``ncaa_mbb_possessions(pbp)``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

import polars as pl

from .identity import enrich_parsed
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data
from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data
from sportsdataverse.mbb.mbb_ncaa_stats_agg import (
    ncaa_mbb_player_stats,
    ncaa_mbb_team_stats,
)
from sportsdataverse.mbb.mbb_shots_adapter import shot_events_to_frame

logger = logging.getLogger(__name__)

__all__ = ["parse_bundle", "write_parsed", "parse_and_write"]

_WBB_PERIOD_MODEL: "tuple[int, int, int]" = (4, 600, 300)
_SEASON_RE = re.compile(r"^(\d{4})-(\d{2})$")

#: The six per-game families, i.e. every key of the parsed record that is a list
#: of game rows. ``teams`` is deliberately absent: it is per-game reference data
#: added by :mod:`ncaa_identity`, not a play/stat family.
PER_GAME_FAMILIES: "tuple[str, ...]" = (
    "pbp",
    "lineups",
    "player_box",
    "team_box",
    "shots",
    "possessions",
)


def _ending_year(season: Optional[str]) -> int:
    """``"2025-26"`` -> ``2026``; falls back to the current year on drift."""
    if season:
        m = _SEASON_RE.match(season)
        if m:
            start, suffix = int(m.group(1)), int(m.group(2))
            end = (start // 100) * 100 + suffix
            return end if end > start else end + 100
        try:
            return int(season)
        except ValueError:
            pass
    return datetime.now().year


def _jsonable(obj: Any) -> Any:
    """Recursively convert a parser-model value tree to plain JSON-able types.

    Handles the shapes the cbb-explorer dataclasses (``LineupEvent`` and its
    nested ``LineupEventStats``/``PlayerCodeId``/etc.) actually carry:
    dataclasses -> dict, ``Enum`` -> ``.value``, ``date``/``datetime`` ->
    ISO string, list/tuple/set/frozenset -> list, dict -> str-keyed dict.
    Everything else (str/int/float/bool/None) passes through unchanged.
    """
    if is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: _jsonable(getattr(obj, f.name)) for f in dc_fields(obj)}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, date):  # datetime is a date subclass
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in obj]
    return obj


def _parse_lineups(contest_id: str, pbp_df: pl.DataFrame, pbp_html: str, stats_html: str) -> "list[dict[str, Any]]":
    """Both teams' box lineups -> stint events (good stints only)."""
    home_team = pbp_df["home"][0]
    away_team = pbp_df["away"][0]
    events: "list[LineupEvent]" = []
    for team_name in (home_team, away_team):
        if not team_name:
            continue
        box_lineup = get_box_lineup(
            f"individual_stats_{contest_id}.html",
            stats_html,
            TeamId(team_name),
            format_version=1,
        )
        if isinstance(box_lineup, list):  # list[ParseError]
            continue
        lineup_result = create_lineup_data(f"pbp_{contest_id}.html", pbp_html, box_lineup, format_version=1)
        if isinstance(lineup_result, list):  # list[ParseError]
            continue
        good, _bad = lineup_result
        events.extend(good)
    return [_jsonable(ev) for ev in events]


def _parse_shots(
    contest_id: str,
    pbp_df: pl.DataFrame,
    stats_html: str,
    box_html: str,
    season: Optional[str],
    league: str,
) -> "list[dict[str, Any]]":
    """The SVG shot map on the ``box_score`` page, keyed off the home team's box lineup."""
    home_team = pbp_df["home"][0]
    if not home_team:
        return []
    box_lineup: Union[LineupEvent, "list[ParseError]"] = get_box_lineup(
        f"individual_stats_{contest_id}.html",
        stats_html,
        TeamId(home_team),
        format_version=1,
    )
    if isinstance(box_lineup, list):
        return []
    shots = create_shot_event_data(f"box_score_{contest_id}.html", box_html, box_lineup)
    if not shots or isinstance(shots[0], ParseError):
        return []
    frame = shot_events_to_frame(
        shots,
        season=_ending_year(season),
        league="womens" if league == "wbb" else "mens",
    )
    return frame.to_dicts()


def parse_bundle(
    bundle: "dict[str, Any]",
    *,
    league: str,
    root: "Optional[Union[str, Path]]" = None,
) -> "dict[str, Any]":
    """Parse one raw captured bundle into the six combined datasets.

    Args:
        bundle: A raw bundle as returned by :func:`ncaa_bundle.read_bundle`
            (``contest_id``, ``season``, ``pages`` with ``play_by_play`` /
            ``box_score`` / ``individual_stats`` HTML).
        league: ``"mbb"`` (default) or ``"wbb"`` -- selects the pbp period
            model (halves vs. quarters) and the shots frame's league label.
        root: Repo root, for the offline identity join against the committed
            ``{league}/rosters/`` + ``{league}/teams/`` trees. Defaults to this
            repo. A season whose trees were never built still parses -- the id
            columns come back as nulls.

    Returns:
        ``{"contest_id": str, "pbp": [...], "lineups": [...], "player_box":
        [...], "team_box": [...], "shots": [...], "possessions": [...],
        "teams": [...]}`` -- every dataset a ``list[dict]``. Any family whose
        parse failed is an empty list (never raises). ``teams`` is the
        two-row readable team identity added by :mod:`ncaa_identity`, which
        also stamps ids + properly-cased names onto the other families.

        EVERY row of the six per-game families carries ``contest_id`` (Utf8,
        equal to the top-level key) -- that is the join key across families,
        and it replaces the parser stack's own ``game_id``.
    """
    contest_id = str(bundle["contest_id"])
    pages = bundle.get("pages") or {}
    pbp_html = pages.get("play_by_play") or ""
    box_html = pages.get("box_score") or ""
    stats_html = pages.get("individual_stats") or ""
    season = bundle.get("season")

    result: "dict[str, Any]" = {
        "contest_id": contest_id,
        "pbp": [],
        "lineups": [],
        "player_box": [],
        "team_box": [],
        "shots": [],
        "possessions": [],
    }

    pbp_df: Optional[pl.DataFrame] = None
    try:
        # Explicit branch rather than **kwargs: unpacking a dict hides which
        # keyword is being set, so a type checker matches it against the wrong
        # parameter (here `fix_technicals: bool`) and the men's path loses its
        # signature check entirely.
        if league == "wbb":
            pbp_df = parse_ncaa_bb_game_pbp(pbp_html, contest_id, period_model=_WBB_PERIOD_MODEL)
        else:
            pbp_df = parse_ncaa_bb_game_pbp(pbp_html, contest_id)
        result["pbp"] = pbp_df.to_dicts()
    except Exception:  # noqa: BLE001 -- one family's failure must not abort the others
        logger.warning("ncaa_parse: family=pbp contest_id=%s failed", contest_id, exc_info=True)

    try:
        if pbp_df is not None and pbp_df.height > 0:
            result["lineups"] = _parse_lineups(contest_id, pbp_df, pbp_html, stats_html)
    except Exception:  # noqa: BLE001
        logger.warning("ncaa_parse: family=lineups contest_id=%s failed", contest_id, exc_info=True)

    try:
        if pbp_df is not None and pbp_df.height > 0:
            result["player_box"] = ncaa_mbb_player_stats(pbp_df).to_dicts()
    except Exception:  # noqa: BLE001
        logger.warning(
            "ncaa_parse: family=player_box contest_id=%s failed",
            contest_id,
            exc_info=True,
        )

    try:
        if pbp_df is not None and pbp_df.height > 0:
            result["team_box"] = ncaa_mbb_team_stats(pbp_df).to_dicts()
    except Exception:  # noqa: BLE001
        logger.warning(
            "ncaa_parse: family=team_box contest_id=%s failed",
            contest_id,
            exc_info=True,
        )

    try:
        if pbp_df is not None and pbp_df.height > 0:
            result["shots"] = _parse_shots(contest_id, pbp_df, stats_html, box_html, season, league)
    except Exception:  # noqa: BLE001
        logger.warning("ncaa_parse: family=shots contest_id=%s failed", contest_id, exc_info=True)

    try:
        if pbp_df is not None and pbp_df.height > 0:
            result["possessions"] = ncaa_mbb_possessions(pbp_df).to_dicts()
    except Exception:  # noqa: BLE001
        logger.warning(
            "ncaa_parse: family=possessions contest_id=%s failed",
            contest_id,
            exc_info=True,
        )

    # ONE per-game identifier column, on every family, named the same thing the
    # top-level key is. The parser stack emits `game_id` on five of the six
    # (`lineups` has none, and `shots` hardcodes it to None -- so reading the
    # families' own value would leave shots unjoinable). Stamping the bundle's
    # contest_id instead makes all six agree by construction. Pure dict work, so
    # deliberately NOT inside a try: it cannot fail and must not be skipped.
    for family in PER_GAME_FAMILIES:
        for row in result[family]:
            row.pop("game_id", None)
            row["contest_id"] = contest_id

    # Identity join LAST, over whatever the six families produced -- a pure
    # additive pass, so a family that failed above simply has no rows to stamp.
    # Its own failure must not lose the parse either.
    try:
        enrich_parsed(
            result,
            root=root if root is not None else Path(__file__).resolve().parents[1],
            league=league,
            season=_ending_year(season),
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "ncaa_parse: identity enrichment contest_id=%s failed",
            contest_id,
            exc_info=True,
        )

    return result


def write_parsed(root: "Union[str, Path]", league: str, contest_id: str, parsed: "dict[str, Any]") -> Path:
    """Write *parsed* to ``root/{league}/json/{contest_id}.json`` (plain utf-8 JSON, atomic)."""
    path = Path(root) / league / "json" / f"{contest_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(parsed), encoding="utf-8")
    os.replace(tmp_path, path)
    return path


def parse_and_write(bundle: "dict[str, Any]", root: "Union[str, Path]", *, league: str) -> Path:
    """Convenience: :func:`parse_bundle` then :func:`write_parsed`."""
    parsed = parse_bundle(bundle, league=league, root=root)
    return write_parsed(root, league, parsed["contest_id"], parsed)


def _parse_shard(spec: str) -> "tuple[int, int]":
    """``"i/N"`` -> ``(i, N)``. Defaults to ``0/1`` (no sharding)."""
    i_str, _, n_str = spec.partition("/")
    i, n = int(i_str), int(n_str or "1")
    if n < 1 or not (0 <= i < n):
        raise ValueError(f"invalid --shard {spec!r}; expected 'i/N' with 0<=i<N")
    return i, n


def _main(default_league: str) -> None:
    import argparse

    from .bundle import read_bundle
    from .capture import shard

    parser = argparse.ArgumentParser(description="Parse raw captured bundles into combined per-contest JSON.")
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Root of the raw data tree (default: repo root).",
    )
    parser.add_argument(
        "--shard",
        default="0/1",
        help="This process's shard as 'i/N' (default: 0/1, no sharding).",
    )
    parser.add_argument("--league", default=default_league, help="League slug (default: mbb).")
    args = parser.parse_args()
    i, n = _parse_shard(args.shard)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    root = Path(args.root)
    raw_dir = root / args.league / "raw"
    bundle_paths = sorted(raw_dir.glob("**/*.json.gz"))

    pending = []
    for p in bundle_paths:
        contest_id = p.name[: -len(".json.gz")]
        if not (root / args.league / "json" / f"{contest_id}.json").exists():
            pending.append(p)

    my_paths = shard([str(p) for p in pending], i, n)
    print(f"bundles={len(bundle_paths)} pending={len(pending)} shard={i}/{n} assigned={len(my_paths)}")

    counts = {"parsed": 0, "failed": 0}
    for path_str in my_paths:
        try:
            bundle = read_bundle(path_str)
            parse_and_write(bundle, root, league=args.league)
            counts["parsed"] += 1
        except Exception:  # noqa: BLE001 -- one bad bundle must not abort the run
            logger.warning("ncaa_parse CLI: failed to parse %s", path_str, exc_info=True)
            counts["failed"] += 1

    print(f"parsed={counts['parsed']} failed={counts['failed']}")
