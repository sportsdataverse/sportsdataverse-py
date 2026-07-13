"""NCAA stats.ncaa.org shot locations — bigballR ``get_shot_locations`` port.

Faithful polars port of bigballR's shot-chart scraper + pbp attach helper:

* ``get_shot_locations`` — ``bigballR/R/get_shot_locations.R:3-89``
  (nested ``process_game`` at ``:7``)
* ``join_pbp_shots``     — ``bigballR/R/get_shot_locations.R:93-135``

The shot chart lives in the ``/contests/{id}/box_score`` page as client-side
JS: one ``addShot(raw_x, raw_y, shooting_org, made, play_id,
'play_description', 'classes', show_highlight);`` call per shot. Raw
coordinates are 0-100 percentages of the court; the output is NCAA court
feet (94x50), ``y`` vertically flipped, with ``shot_dist`` measured from the
basket center inferred per (game, team, period) side heuristic
(bigballR's 5.25 / 88.75 ft baskets, ``by = 25``).

**WBB extension status**: wbigballR keeps both functions in an unexported
``get_shot_locations.R`` rewrite (no ``join_pbp_shots`` export, cruder
``4 / 90`` basket constants, and MBB-halves ``Game_Seconds`` math applied to
quarter games) — there is therefore NO women's R oracle. This module is the
deliberate Python extension for WBB (divergence spec §2.9 / §3 ``basket_xy``
knob): the shared-court-geometry bigballR constants (5.25 / 88.75) are used
for both leagues, and the ``period_model`` knob — same shape as
:mod:`~sportsdataverse.mbb.mbb_ncaa_game_pbp` — supplies correct WBB quarter
seconds ``(4, 600, 300)`` in place of the R halves ``case_when``.

Deliberate R-faithful quirks are marked with ``ponytail:`` comments citing
the R line they reproduce.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Literal, Optional, Protocol, Sequence, Union, overload

import polars as pl

logger = logging.getLogger(__name__)

__all__ = [
    "SHOTS_RENAME",
    "SHOTS_SCHEMA",
    "parse_ncaa_bb_shots",
    "ncaa_mbb_shot_locations",
    "ncaa_mbb_join_pbp_shots",
]

#: bigballR ``get_shot_locations`` output contract → sdv-py snake_case
#: (``Half_Status`` → ``period`` / ``Time`` → ``clock``, mirroring the pbp
#: contract in ``tests/mbb/_bigballr_oracle.PBP_RENAME``).
SHOTS_RENAME: "dict[str, str]" = {
    "ID": "game_id",
    "Half_Status": "period",
    "Time": "clock",
    "Game_Seconds": "game_seconds",
    "Team": "team",
    "Player": "player",
    "Shot_Result": "shot_result",
    "x": "x",
    "y": "y",
    "Shot_Dist": "shot_dist",
}

#: Output contract dtypes. ``game_id`` stays Utf8 (opaque contest id).
SHOTS_SCHEMA: "dict[str, type[pl.DataType]]" = {
    "game_id": pl.Utf8,
    "period": pl.Int64,
    "clock": pl.Utf8,
    "game_seconds": pl.Int64,
    "team": pl.Utf8,
    "player": pl.Utf8,
    "shot_result": pl.Utf8,
    "x": pl.Float64,
    "y": pl.Float64,
    "shot_dist": pl.Float64,
}

#: ``(n_regulation_periods, regulation_period_seconds, ot_period_seconds)``
#: — MBB halves; WBB quarters are ``(4, 600, 300)``.
_MBB_PERIOD_MODEL: "tuple[int, int, int]" = (2, 1200, 300)

#: One shot per line: ``addShot(<8 comma-separated fields>);`` (R ``:14``,
#: PCRE ``(?<=addShot\().+?(?=\);)`` per line — capture-group rewrite, no
#: lookaround). ``.`` does not cross newlines, matching R's per-line scan.
_ADD_SHOT_RE = re.compile(r"addShot\((.+?)\);")

#: Suffix-comma fixes BEFORE the field split, in R's exact order (``:17-21``;
#: the ``", II"`` rule intentionally shadows ``", III"``/``", IV"``).
_SUFFIX_FIXES: "tuple[tuple[str, str], ...]" = (
    (", Jr.", " Jr."),
    (", JR.", " JR."),
    (", II", " II"),
    (", III", " III"),
    (", IV", " IV"),
)

#: FIRST digit in the description = period ordinal ("1st ..." → 1) (R ``:34``).
_DIGIT_RE = re.compile(r"\d")

#: First ``MM:SS`` in the description ("19:45:00" → "19:45") (R ``:41-43``).
_CLOCK_RE = re.compile(r"(\d{2}):(\d{2})")

#: ``... missed|made by NAME(`` — lazy up to the first ``(`` (R ``:35``,
#: lookaround ``(?<= (missed|made) by ).+?(?=\()`` rewritten as a group).
_PLAYER_RE = re.compile(r" (?:missed|made) by (.+?)\(")

#: GREEDY first-``(`` .. last-``)`` so nested team parens survive, outer pair
#: stripped by the capture (R ``:36-39``, the issue96 fix).
_TEAM_RE = re.compile(r"\((.*)\)")

#: Basket centers: 5.25 ft from each baseline on the 94-ft axis, mid-court on
#: the 50-ft axis (R ``:82-83``; adopted for BOTH leagues — divergence spec §3
#: ``basket_xy``: wbigballR's 4 / 90 is imprecision, not domain).
_BASKET_X_LEFT = 5.25
_BASKET_X_RIGHT = 88.75
_BASKET_Y = 25.0


def _float_or_none(x: str) -> Optional[float]:
    """R ``as.numeric`` — unparseable becomes missing, never raises."""
    try:
        return float(x)
    except ValueError:
        return None


def _empty_shots() -> pl.DataFrame:
    return pl.DataFrame(schema=SHOTS_SCHEMA)


def parse_ncaa_bb_shots(
    html: str,
    game_id: str,
    *,
    period_model: "tuple[int, int, int]" = _MBB_PERIOD_MODEL,
) -> pl.DataFrame:
    """Parse one box-score page's ``addShot`` JS calls into the shots frame.

    Pure core of bigballR ``get_shot_locations``'s nested ``process_game``
    (``get_shot_locations.R:7-59``) plus the post-bind side/distance block
    (``:77-86`` — the (team, period) grouping there includes the game id, so
    computing it per game is identical to R's after-bind computation).

    Args:
        html: Raw ``stats.ncaa.org/contests/{game_id}/box_score`` HTML.
        game_id: NCAA contest id (kept as Utf8 passthrough).
        period_model: ``(n_regulation_periods, regulation_period_seconds,
            overtime_period_seconds)``. MBB halves ``(2, 1200, 300)`` —
            reproducing the R ``case_when`` exactly for periods 1-8; WBB
            quarters ``(4, 600, 300)`` (Python extension — wbigballR's
            unexported copy wrongly keeps the halves math).

    Returns:
        One row per charted shot with the :data:`SHOTS_SCHEMA` columns
        (``game_id``, ``period``, ``clock``, ``game_seconds``, ``team``,
        ``player``, ``shot_result``, ``x``, ``y``, ``shot_dist``). A page
        with no shot chart returns a zero-row frame with the documented
        schema (R errors there; graceful-empty per repo convention).

    Example:
        Quick start::

            from pathlib import Path
            from sportsdataverse.mbb.mbb_ncaa_shots import parse_ncaa_bb_shots
            html = Path("box_6470186.html").read_text(encoding="utf-8")
            df = parse_ncaa_bb_shots(html, "6470186")
            print(df.shape)

        WBB quarters (Python extension — no wbigballR export)::

            df = parse_ncaa_bb_shots(html, "5722355", period_model=(4, 600, 300))

        Pipeline next step (one line)::

            df.filter(pl.col("shot_result") == "made").head()

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)
        * `wehoop`_ -- women's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    n_reg, reg_len, ot_len = period_model

    periods: "list[Optional[int]]" = []
    clocks: "list[Optional[str]]" = []
    game_seconds: "list[Optional[int]]" = []
    teams: "list[Optional[str]]" = []
    players: "list[Optional[str]]" = []
    results: "list[str]" = []
    xs: "list[Optional[float]]" = []
    ys: "list[Optional[float]]" = []

    for call in _ADD_SHOT_RE.findall(html):
        text = call
        for old, new in _SUFFIX_FIXES:
            text = text.replace(old, new)  # R gsub = replace-all (:17-21)
        parts = text.split(", ")
        if len(parts) != 8:
            # R's strsplit + rbind would mangle the whole game here; skip the
            # one bad row instead and let parity tests surface any real drift.
            logger.warning("game %s: skipping malformed addShot call: %r", game_id, call)
            continue

        raw_x = _float_or_none(parts[0])
        raw_y = _float_or_none(parts[1])
        desc = parts[5]  # single quotes kept, exactly as R's field split sees it

        digit_m = _DIGIT_RE.search(desc)
        period = int(digit_m.group(0)) if digit_m else None

        clock_m = _CLOCK_RE.search(desc)
        if clock_m is None or period is None:
            clock = None
            secs: Optional[int] = None
        else:
            clock = clock_m.group(0)
            elapsed_end = min(period, n_reg) * reg_len + max(period - n_reg, 0) * ot_len
            # ponytail: R (:45-53) hardcodes halves 1200/2400 then flat 4200
            # for period >= 8; the formula agrees through period 8 and only
            # diverges at the unreachable 7-OT+ tail.
            secs = elapsed_end - (int(clock_m.group(1)) * 60 + int(clock_m.group(2)))

        player_m = _PLAYER_RE.search(desc)
        team_m = _TEAM_RE.search(desc)
        team = team_m.group(1) if team_m else None
        if team is not None:
            team = team.replace("&amp;", "&", 1)  # R str_replace = first hit (:36-39)

        periods.append(period)
        clocks.append(clock)
        game_seconds.append(secs)
        teams.append(team)
        players.append(player_m.group(1) if player_m else None)
        results.append("made" if parts[3] == "true" else "missed")  # R :44
        xs.append(raw_x / 100 * 94 if raw_x is not None else None)  # R :32
        ys.append(50 - raw_y / 100 * 50 if raw_y is not None else None)  # R :33

    if not periods:
        return _empty_shots()

    df = pl.DataFrame(
        {
            "game_id": [game_id] * len(periods),
            "period": periods,
            "clock": clocks,
            "game_seconds": game_seconds,
            "team": teams,
            "player": players,
            "shot_result": results,
            "x": xs,
            "y": ys,
        },
        schema={k: v for k, v in SHOTS_SCHEMA.items() if k != "shot_dist"},
    )

    # Side heuristic per (team, period): "right" needs >= 3 shots with x > 50
    # (R :78-79; game_id is constant within one parse, so the R group
    # (ID, Team, Half_Status) reduces to (team, period) here).
    # ponytail: a null x makes R's sum() NA and NAs the whole group's
    # Shot_Dist; polars sum skips nulls — unreachable divergence, chart rows
    # always carry numeric coordinates.
    side_right = (pl.col("x") > 50).sum().over(["team", "period"]) > 2
    bx = pl.when(side_right).then(pl.lit(_BASKET_X_RIGHT)).otherwise(pl.lit(_BASKET_X_LEFT))
    shot_dist = ((pl.col("x") - bx) ** 2 + (pl.col("y") - _BASKET_Y) ** 2).sqrt()
    return df.with_columns(shot_dist.alias("shot_dist")).select(list(SHOTS_SCHEMA))


class _SupportsFetchGameBox(Protocol):
    """Duck-typed fetcher surface used by :func:`ncaa_mbb_shot_locations`."""

    def fetch_game_box(self, contest_id: object) -> str: ...  # pragma: no cover


@overload
def ncaa_mbb_shot_locations(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGameBox] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_shot_locations(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGameBox] = ...,
    return_as_pandas: Literal[True],
) -> Any: ...


def ncaa_mbb_shot_locations(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGameBox] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """Scrape MBB shot locations for one or more games (bigballR
    ``get_shot_locations``, ``get_shot_locations.R:3-89``).

    Fetches each game's ``stats.ncaa.org/contests/{id}/box_score`` page and
    parses the embedded shot-chart JS through :func:`parse_ncaa_bb_shots`.
    NA ids are dropped up front (R ``:5``); per-game "shots found" messages
    go to the module logger (R ``message``, ``:69-70``).

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        fetcher: Optional injected fetcher exposing ``fetch_game_box`` (for
            tests/offline use). Defaults to a fresh
            ``NcaaFetcher.with_browser()`` context per call.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        All games' shots row-bound (zero-row :data:`SHOTS_SCHEMA` frame when
        no ids survive or no charts are found).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_shots import ncaa_mbb_shot_locations
            df = ncaa_mbb_shot_locations(["6470186", "6479639"])
            print(df.shape)

        Offline with an injected fetcher::

            df = ncaa_mbb_shot_locations(["6470186"], fetcher=my_fetcher)

        Pipeline next step (one line)::

            df.group_by("team").agg(pl.col("shot_dist").mean()).head()

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    ids = [g for g in game_ids if g is not None and g == g]  # R :5 drops NAs

    def _run(f: _SupportsFetchGameBox) -> "list[pl.DataFrame]":
        frames: "list[pl.DataFrame]" = []
        for gid in ids:
            df = parse_ncaa_bb_shots(f.fetch_game_box(gid), str(gid))
            found = sorted({t for t in df["team"].to_list() if t is not None})
            logger.info(
                "Game_ID: %s || %s v. %s || %d shots found",
                gid,
                found[0] if found else None,
                found[1] if len(found) > 1 else None,
                df.height,
            )
            frames.append(df)
        return frames

    if fetcher is None:
        from .mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as browser_fetcher:
            frames = _run(browser_fetcher)
    else:
        frames = _run(fetcher)

    out = pl.concat(frames) if frames else _empty_shots()
    return out.to_pandas() if return_as_pandas else out


@overload
def ncaa_mbb_join_pbp_shots(
    pbp: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_join_pbp_shots(
    pbp: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    return_as_pandas: Literal[True],
) -> Any: ...


@overload
def ncaa_mbb_join_pbp_shots(
    pbp: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]": ...
def ncaa_mbb_join_pbp_shots(
    pbp: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """Attach shot-chart coordinates to play-by-play rows (bigballR
    ``join_pbp_shots``, ``get_shot_locations.R:93-135``).

    FG attempts (``shot_value`` 2/3) are matched to chart shots on
    ``(game_id, game_seconds, event_result == shot_result, shot_no)`` where
    ``shot_no`` is the within-second same-result sequence number on BOTH
    sides — free throws and non-shot rows are deliberately excluded from
    matching (the chart plots FGs only) and pass through NA-filled. Row
    count and per-game row order are preserved; the explicit ``shot_dist``
    carry-through is fork-skew fix #11 (wbigballR's unexported copy drops
    it). Works identically for the WBB extension — feed it quarter-model
    pbp + shots frames.

    Args:
        pbp: The 35-column snake_case pbp contract frame (see
            :data:`~sportsdataverse.mbb.mbb_ncaa_game_pbp.PBP_SCHEMA`).
        shots: A :data:`SHOTS_SCHEMA` frame (from
            :func:`parse_ncaa_bb_shots` / :func:`ncaa_mbb_shot_locations`)
            covering exactly the same game ids.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input pbp frame + ``team``, ``player``, ``x``, ``y``,
        ``shot_dist`` (null on non-FG rows and unmatched FG rows), sorted by
        (game_id, original per-game row order) exactly as R's
        ``arrange(row, .by_group = TRUE)``.

    Raises:
        ValueError: If the pbp and shots game-id sets differ (R
            ``stop("PBP and Shot Locations do not match.")``, ``:97-99``),
            or if a join-key dtype disagrees between the two frames.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
            from sportsdataverse.mbb.mbb_ncaa_shots import (
                ncaa_mbb_join_pbp_shots,
                ncaa_mbb_shot_locations,
            )
            pbp = ncaa_mbb_play_by_play(["6470186"])
            shots = ncaa_mbb_shot_locations(["6470186"])
            joined = ncaa_mbb_join_pbp_shots(pbp, shots)

        Pipeline next step (one line)::

            joined.filter(pl.col("x").is_not_null()).head()

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # R sort(unique(...)) drops NAs before the identical() guard (:94-99).
    pbp_ids = sorted(pbp["game_id"].drop_nulls().unique().to_list())
    shot_ids = sorted(shots["game_id"].drop_nulls().unique().to_list())
    if pbp_ids != shot_ids:
        raise ValueError("PBP and Shot Locations do not match.")

    for left_key, right_key in (
        ("game_id", "game_id"),
        ("game_seconds", "game_seconds"),
        ("event_result", "shot_result"),
    ):
        if pbp.schema[left_key] != shots.schema[right_key]:
            raise ValueError(
                f"join key dtype mismatch: pbp[{left_key!r}]={pbp.schema[left_key]} "
                f"!= shots[{right_key!r}]={shots.schema[right_key]}"
            )

    pbp_rowed = pbp.with_columns(pl.int_range(pl.len()).over("game_id").alias("__row"))
    shot_att = pbp_rowed.filter(pl.col("shot_value").is_in([2, 3]))
    no_shot_att = pbp_rowed.filter(pl.col("shot_value").is_null() | (pl.col("shot_value") == 1))

    shot_att = shot_att.with_columns(
        pl.int_range(pl.len()).over(["game_id", "game_seconds", "event_result"]).alias("__shot_no")
    )
    shots_keyed = shots.with_columns(
        pl.int_range(pl.len()).over(["game_id", "game_seconds", "shot_result"]).alias("__shot_no")
    ).select(
        "game_id",
        "game_seconds",
        pl.col("shot_result").alias("event_result"),
        "__shot_no",
        "team",
        "player",
        "x",
        "y",
        "shot_dist",
    )

    # ponytail: dplyr left_join matches NA keys to NA keys; polars does not.
    # The shots side never carries null keys (result/clock always parse), so
    # both engines produce "no match" for a null pbp key — equivalent here.
    joined = shot_att.join(
        shots_keyed,
        on=["game_id", "game_seconds", "event_result", "__shot_no"],
        how="left",
    )

    combined = pl.concat([joined, no_shot_att], how="diagonal").sort(["game_id", "__row"]).drop(["__row", "__shot_no"])
    return combined.to_pandas() if return_as_pandas else combined
