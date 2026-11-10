# sportsdataverse/nba/nba_game_expected.py
"""Game expected points by zone and situation for one NBA, WNBA, men's or women's college game (BB-D40).

:func:`team_game_expected` puts the shot-quality model's expected points (BB-D9, sdv-db
``shot_quality_game``) on every field-goal attempt of the ESPN play-by-play and adds them up over
the possessions and zones of the BB-D31 splits (:mod:`sportsdataverse.nba.nba_game_splits`), so it
works mid-game; :func:`~sportsdataverse.nba.nba_game_page.game_page_tables` (and so
``game_page_frames``) returns it as ``team_game_expected``.

**The expectation** (:func:`shot_xfg`) is BB-D9's, term for term. A shot's cell is its 2.5 ft
league square ``(i, j) = floor(x_legacy / 25 + 0.5), floor(y_legacy / 25 + 0.5)`` split by shot
value and whole-foot distance; its make chance is the league's make rate in that cell, shrunk
toward its distance ring (same value, same foot), shrunk toward its shot value::

    p_ring = (m_ring + ring_k * m_value / n_value) / (n_ring + ring_k)
    xfg    = (m_cell + square_k * p_ring) / (n_cell + square_k)

with :data:`SHOT_QUALITY_K`'s fitted ``(square_k, ring_k)``. The counts are the league's shots in
games on the :data:`WINDOW_DAYS` days before the game date (never the game's own day):
:func:`shot_cells` makes them from the league's shot history (the BB-D9 source rows, the
stats.nba.com / stats.wnba.com shots for the pros and the NCAA shot charts for college) as of one
date. An ESPN shot's distance is the one its text gives (``"misses 23-foot three point"``, the
league feed's own whole feet), else ``floor(hypot(x_legacy, y_legacy) / 10 + 0.5)`` (the college
shot rows' rule; the pros' feed rounds the same way on 99.6% of the WNBA's 2024-25 shots). An
attempt without a location on the court gets its shot value's league make rate.

ESPN places a shot to the whole foot, the league feeds to the inch, so a game's expected points
from the ESPN plays track BB-D9's total from the feed's shots of the same game, not to the
digit: on 38 WNBA 2025 regular-season games the ratio ran 0.978-1.006 (mean 0.996), with the
same field-goal attempts in every game (``tests/fixtures/bb_game_expected/README.md``).

**Situations** are possession families, each with ``_poss``, ``_rate`` (share of possessions),
``_pts``, ``_ppp`` and ``_poe`` (the field-goal points over expected of its attempts):
``second_chance`` and ``fast_break`` are BB-D31's second-chance and transition possessions,
``off_turnover`` started on any opponent turnover (live or dead), ``paint`` took a shot at the rim
or in the short midrange.

**Zones** reuse the season profile's classifiers (BB-D35) on the play coordinates. The pros use
:func:`~sportsdataverse.nba.nba_shot_zones.add_shot_zones`: ``rim`` the restricted area (under 4
ft), ``short_mid`` 4 to under 8 ft and within 8 ft of the hoop to either side,
``midrange`` any other two, ``corner3`` a three in the band below the break (also 22 ft out for
the NBA) and ``above_break3`` any other three. College uses the NCAA shot chart's geometry
(:func:`~sportsdataverse.mbb.mbb_shot_quality_constants.get_constants`): ``rim`` within 4 ft,
``short_mid`` within 15 ft, ``midrange`` beyond, ``corner3`` 21 ft or more to the side within 9
ft of the baseline. A shot's value is the play's own. Each zone has ``_fga``, ``_fgm``,
``_efg_pct``, ``_xppp`` (expected points per attempt) and ``_poe``; the five zones and
``unlocated_fga`` / ``unlocated_fgm`` add up to ``fga`` / ``fgm``.

**Totals**: ``fg_pts`` / ``xfg_pts`` / ``poe`` over the field goals, ``efg_pct`` / ``xefg_pct``,
``ts_pct`` / ``xts_pct`` (expected field-goal points plus the free throws made, over twice FGA +
0.44 FTA), ``ortg`` / ``xortg`` (points and ``points - poe`` per 100 possessions) and ``drtg`` /
``xdrtg``, the opponent's ``ortg`` / ``xortg``. Without ``cells`` every expected column is null.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants
from sportsdataverse.nba.nba_game_page import _box_season, _ratio, _resolve_league, game_regulation_periods
from sportsdataverse.nba.nba_game_splits import _possessions, _split_events
from sportsdataverse.nba.nba_shot_zones import add_shot_zones

__all__ = [
    "SHOT_QUALITY_K",
    "SITUATIONS",
    "WINDOW_DAYS",
    "ZONES",
    "shot_cells",
    "shot_xfg",
    "team_game_expected",
]

#: ``(square_k, ring_k)`` per league: sdv-db ``shot_quality.SOURCES``, the argmin of training-season
#: Brier (``tests/fixtures/shot_quality/fit_<schema>.json`` there; NBA train 2022-23, WNBA 2019-21,
#: college 2023-24). A refit there is a change here.
SHOT_QUALITY_K: dict[str, tuple[float, float]] = {
    "nba": (800.0, 0.1),
    "wnba": (400.0, 0.5),
    "mbb": (400.0, 5.0),
    "wbb": (800.0, 10.0),
}
#: The as-of window: the league's shots in games on the 365 days before the game date.
WINDOW_DAYS: int = 365
_SQUARE_SIDE = 25.0  # tenths of a foot (BB-D1's lattice)
_COURT_HALF_WIDTH, _COURT_BASELINE_Y = 250.0, -52.5  # sdv-db ON_COURT
ZONES: tuple[str, ...] = ("rim", "short_mid", "midrange", "corner3", "above_break3")
SITUATIONS: tuple[str, ...] = ("second_chance", "off_turnover", "fast_break", "paint")
_PRO_ZONE = {
    "restricted_area": "rim",
    "in_the_paint_non_ra": "short_mid",
    "mid_range": "midrange",
    "corner_3": "corner3",
    "above_the_break_3": "above_break3",
}
#: The WNBA keeps the corner band test alone (BB-D35's ``_CORNER_MIN_ABS_X``).
_CORNER_MIN_ABS_X = {"nba": 220.0, "wnba": 0.0}
_COLLEGE = {"mbb": "mens", "wbb": "womens"}

CELLS_SCHEMA: dict[str, pl.DataType] = {
    "kind": pl.Utf8,
    "shot_value": pl.Int64,
    "shot_distance": pl.Int64,
    "i": pl.Int64,
    "j": pl.Int64,
    "n": pl.Float64,
    "m": pl.Float64,
}
_KEYS = {
    "cell": ["i", "j", "shot_value", "shot_distance"],
    "ring": ["shot_value", "shot_distance"],
    "value": ["shot_value"],
}

_COUNTS = ("fga", "fgm", "fg3a", "fg3m", "fta", "ftm", "points", "possessions", "fg_pts")
TEAM_EXPECTED_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "team_id": pl.Int32,
    **dict.fromkeys(_COUNTS, pl.Int64),
    **dict.fromkeys(
        (
            "xfg_pts",
            "poe",
            "poe_per_shot",
            "efg_pct",
            "xefg_pct",
            "ts_pct",
            "xts_pct",
            "ortg",
            "xortg",
            "drtg",
            "xdrtg",
        ),
        pl.Float64,
    ),
    **{
        f"{s}_{m}": dt_
        for s in SITUATIONS
        for m, dt_ in (
            ("poss", pl.Int64),
            ("rate", pl.Float64),
            ("pts", pl.Int64),
            ("ppp", pl.Float64),
            ("poe", pl.Float64),
        )
    },
    **{
        f"{z}_{m}": dt_
        for z in ZONES
        for m, dt_ in (
            ("fga", pl.Int64),
            ("fgm", pl.Int64),
            ("efg_pct", pl.Float64),
            ("xppp", pl.Float64),
            ("poe", pl.Float64),
        )
    },
    "unlocated_fga": pl.Int64,
    "unlocated_fgm": pl.Int64,
}
#: The columns that need the league's cells (null without them).
_EXPECTED_COLS: tuple[str, ...] = tuple(
    c for c in TEAM_EXPECTED_SCHEMA if c.startswith("x") or c.endswith(("_poe", "_xppp", "poe_per_shot")) or c == "poe"
)


def _cell_keys(shots: pl.DataFrame) -> pl.DataFrame:
    """``i`` / ``j`` (the 2.5 ft square), and ``_on_court`` for located shots inside the court frame."""
    x, y = pl.col("x_legacy").cast(pl.Float64), pl.col("y_legacy").cast(pl.Float64)
    return shots.with_columns(
        (x / _SQUARE_SIDE + 0.5).floor().cast(pl.Int64).alias("i"),
        (y / _SQUARE_SIDE + 0.5).floor().cast(pl.Int64).alias("j"),
        ((x.abs() <= _COURT_HALF_WIDTH) & (y >= _COURT_BASELINE_Y) & pl.col("shot_distance").is_not_null())
        .fill_null(False)
        .alias("_on_court"),
    )


def shot_cells(history: pl.DataFrame, as_of: dt.date) -> pl.DataFrame:
    """The league's as-of shot counts behind :func:`shot_xfg`, for games on one date.

    Args:
        history: The league's shots in the BB-D9 source columns: ``game_date`` (Date, or
            ``YYYY-MM-DD`` text), ``shot_value``, ``shot_distance``, ``x_legacy``, ``y_legacy``
            (tenths of a foot, hoop at the origin) and ``made`` (Boolean) or ``shot_result``
            (``"Made"`` / ``"Missed"``). Rows outside the window are ignored.
        as_of: The date of the games to score; only shots of the :data:`WINDOW_DAYS` days before
            it count.

    Returns:
        :data:`CELLS_SCHEMA` rows: ``kind`` ``"cell"`` (by ``i``, ``j``, ``shot_value``,
        ``shot_distance``), ``"ring"`` (by ``shot_value``, ``shot_distance``) and ``"value"`` (by
        ``shot_value``), each with its shots ``n`` and makes ``m``; empty with no shot in the window.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nba.nba_game_expected import shot_cells
            cells = shot_cells(history, dt.date(2025, 7, 1))  # history: the league's dated shots
            cells.filter(cells["kind"] == "value")

        See Also:
            * :func:`shot_xfg` -- scores shots against these counts.
    """
    h = history
    if h.schema.get("game_date") == pl.Utf8:
        h = h.with_columns(pl.col("game_date").str.slice(0, 10).str.to_date(strict=False))
    if "made" not in h.columns:
        h = h.with_columns((pl.col("shot_result") == "Made").alias("made"))
    h = _cell_keys(
        h.filter(
            pl.col("game_date") < as_of,
            pl.col("game_date") >= as_of - dt.timedelta(days=WINDOW_DAYS),
            pl.col("shot_value").is_in([2, 3]),
        ).with_columns(pl.col("shot_value", "shot_distance").cast(pl.Int64))
    ).filter(pl.col("_on_court"))
    parts = [
        h.group_by(keys)
        .agg(pl.len().cast(pl.Float64).alias("n"), pl.col("made").sum().cast(pl.Float64).alias("m"))
        .select(
            pl.lit(kind).alias("kind"),
            *[pl.col(c) if c in keys else pl.lit(None, pl.Int64).alias(c) for c in _KEYS["cell"]],
            "n",
            "m",
        )
        for kind, keys in _KEYS.items()
    ]
    return pl.concat(parts).select([pl.col(c).cast(t) for c, t in CELLS_SCHEMA.items()])


def shot_xfg(shots: pl.DataFrame, cells: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """Each shot's make chance from the league's as-of counts (BB-D9's expectation).

    Args:
        shots: Field-goal attempts with ``shot_value`` (2 or 3), ``shot_distance`` (whole feet),
            ``x_legacy`` and ``y_legacy`` (null when unlocated).
        cells: :func:`shot_cells` as of the shots' game date.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (its :data:`SHOT_QUALITY_K`).

    Returns:
        ``shots`` plus ``xfg`` (Float64): the shrunk cell rate for a located shot on the court,
        the shot value's league make rate for any other, null when the window holds no shot of
        that value.

    Raises:
        ValueError: If the league is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_expected import shot_cells, shot_xfg
            scored = shot_xfg(shots, shot_cells(history, game_date), league="wnba")
            (scored["xfg"] * scored["shot_value"]).sum()  # expected field-goal points

        See Also:
            * :func:`team_game_expected` -- the per-team game frame built on it.
    """
    if league not in SHOT_QUALITY_K:
        raise ValueError(f"league must be one of {sorted(SHOT_QUALITY_K)}, got {league!r}")
    square_k, ring_k = SHOT_QUALITY_K[league]
    out = _cell_keys(shots.with_columns(pl.col("shot_value", "shot_distance").cast(pl.Int64)))
    for kind, p in (("cell", "c"), ("ring", "r"), ("value", "v")):
        keys = _KEYS[kind]
        part = cells.filter(pl.col("kind") == kind).select(
            *keys, pl.col("n").alias(f"_{p}n"), pl.col("m").alias(f"_{p}m")
        )
        out = out.join(part, on=keys, how="left", maintain_order="left")
    n0 = lambda c: pl.col(c).fill_null(0.0)  # noqa: E731 - a cell or ring the window never saw
    p_value = pl.when(pl.col("_vn") > 0).then(pl.col("_vm") / pl.col("_vn"))
    p_ring = (n0("_rm") + ring_k * p_value) / (n0("_rn") + ring_k)
    xfg = (n0("_cm") + square_k * p_ring) / (n0("_cn") + square_k)
    return out.with_columns(pl.when(pl.col("_on_court")).then(xfg).otherwise(p_value).alias("xfg")).drop(
        "i", "j", "_on_court", "_cn", "_cm", "_rn", "_rm", "_vn", "_vm"
    )


def _zone(shots: pl.DataFrame, lg: str, located: pl.Expr) -> pl.Expr:
    """The BB-D35 classifier's zone of each located attempt, named as :data:`ZONES`; null when unlocated."""
    if lg in _COLLEGE:
        c = get_constants(_COLLEGE[lg])
        x, y, dist = pl.col("x_legacy").abs() / 10.0, pl.col("y_legacy").abs() / 10.0, pl.col("_dist_ft")
        three = pl.col("shot_value") == 3
        zone = (
            pl.when(three & (x >= c.corner_x_ft) & (y <= c.corner_y_ft))
            .then(pl.lit("corner3"))
            .when(three)
            .then(pl.lit("above_break3"))
            .when(dist <= c.rim_radius_ft)
            .then(pl.lit("rim"))
            .when(dist <= c.paint_radius_ft)
            .then(pl.lit("short_mid"))
            .otherwise(pl.lit("midrange"))
        )
    else:
        zone = pl.col("shot_zone").replace_strict(_PRO_ZONE, default=None)
    return pl.when(located).then(zone).alias("zone")


def _scored_shots(rows: pl.DataFrame, lg: str, cells: pl.DataFrame | None) -> pl.DataFrame:
    """The game's field-goal attempts with ``shot_value``, ``zone``, ``pts`` and ``xpts``."""
    x, y = pl.col("x_legacy").cast(pl.Float64), pl.col("y_legacy").cast(pl.Float64)
    located = x.is_not_null() & y.is_not_null()
    shots = rows.filter(pl.col("_fga") & pl.col("_poss").is_not_null()).with_columns(
        pl.when(pl.col("_three")).then(3).otherwise(2).cast(pl.Int64).alias("shot_value"),
        ((x**2 + y**2).sqrt() / 10.0).alias("_dist_ft"),
        pl.lit(1).alias("is_field_goal"),
    )
    shots = shots.with_columns(
        pl.coalesce(
            pl.col("text").str.extract(r"(\d+)-foot", 1).cast(pl.Int64, strict=False),
            (pl.col("_dist_ft") + 0.5).floor().cast(pl.Int64),
        ).alias("shot_distance")
    )
    if lg not in _COLLEGE:
        shots = add_shot_zones(shots, corner_min_abs_x=_CORNER_MIN_ABS_X[lg])
    shots = shots.with_columns(
        _zone(shots, lg, located),
        pl.when(pl.col("_made")).then(pl.col("shot_value")).otherwise(0).cast(pl.Int64).alias("pts"),
    )
    if cells is None:
        return shots.with_columns(pl.lit(None, pl.Float64).alias("xpts"))
    return shot_xfg(shots, cells, league=lg).with_columns((pl.col("xfg") * pl.col("shot_value")).alias("xpts"))


def _xsum(expr: pl.Expr) -> pl.Expr:
    """``sum`` of an expected column, null when any attempt has no expectation (rather than a partial sum)."""
    return pl.when(expr.is_null().any()).then(None).otherwise(expr.sum()).cast(pl.Float64)


def _expected_rows(rows: pl.DataFrame, poss: pl.DataFrame, shots: pl.DataFrame) -> pl.DataFrame:
    """Per offense team: the counts, the four situations and the five zones, before the rates."""
    paint = shots.group_by("_poss").agg(pl.col("zone").is_in(["rim", "short_mid"]).any().alias("paint"))
    poss = poss.join(paint, on="_poss", how="left").with_columns(
        pl.col("paint").fill_null(False),
        pl.col("start_type").is_in(["live_turnover", "dead_turnover"]).alias("off_turnover"),
        pl.col("transition").alias("fast_break"),
    )
    sit_poe = (
        shots.join(poss.select("_poss", *SITUATIONS), on="_poss")
        .with_columns((pl.col("pts") - pl.col("xpts")).alias("_poe"))
        .group_by(pl.col("_team").alias("team_id"))
        .agg(*[_xsum(pl.col("_poe").filter(pl.col(s))).alias(f"{s}_poe") for s in SITUATIONS])
    )
    by_team = poss.group_by(pl.col("offense").alias("team_id")).agg(
        pl.len().cast(pl.Int64).alias("possessions"),
        *[pl.col(s).sum().cast(pl.Int64).alias(f"{s}_poss") for s in SITUATIONS],
        *[pl.col("pts").filter(pl.col(s)).sum().cast(pl.Int64).alias(f"{s}_pts") for s in SITUATIONS],
    )
    made, z = pl.col("_made"), pl.col("zone")
    shot_rows = shots.group_by(pl.col("_team").alias("team_id")).agg(
        pl.len().cast(pl.Int64).alias("fga"),
        made.sum().cast(pl.Int64).alias("fgm"),
        (pl.col("shot_value") == 3).sum().cast(pl.Int64).alias("fg3a"),
        ((pl.col("shot_value") == 3) & made).sum().cast(pl.Int64).alias("fg3m"),
        pl.col("pts").sum().cast(pl.Int64).alias("fg_pts"),
        _xsum(pl.col("xpts")).alias("xfg_pts"),
        *[(z == n).sum().cast(pl.Int64).alias(f"{n}_fga") for n in ZONES],
        *[((z == n) & made).sum().cast(pl.Int64).alias(f"{n}_fgm") for n in ZONES],
        *[pl.col("pts").filter(z == n).sum().cast(pl.Int64).alias(f"_{n}_pts") for n in ZONES],
        *[_xsum(pl.col("xpts").filter(z == n)).alias(f"_{n}_xpts") for n in ZONES],
        z.is_null().sum().cast(pl.Int64).alias("unlocated_fga"),
        (z.is_null() & made).sum().cast(pl.Int64).alias("unlocated_fgm"),
    )
    ft = (
        rows.filter(pl.col("_ft"))
        .group_by(pl.col("_team").alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("fta"), pl.col("_made").sum().cast(pl.Int64).alias("ftm"))
    )
    home, away = int(rows["_home"][0]), int(rows["_away"][0])
    total = rows.select(pl.col("_dh").sum(), pl.col("_da").sum()).row(0)
    out = pl.DataFrame(
        {"team_id": [home, away], "points": [int(total[0]), int(total[1])]},
        schema={"team_id": pl.Int32, "points": pl.Int64},
    )
    for part in (by_team, shot_rows, ft, sit_poe):
        out = out.join(
            part.with_columns(pl.col("team_id").cast(pl.Int32)), on="team_id", how="left", maintain_order="left"
        )
    counts = [c for c, t in TEAM_EXPECTED_SCHEMA.items() if t == pl.Int64 and c in out.columns]
    return out.with_columns(
        pl.col(counts).fill_null(0), pl.lit(int(rows["game_id"][0]), dtype=pl.Int32).alias("game_id")
    )


def team_game_expected(
    pbp: pl.DataFrame,
    player_box: pl.DataFrame | None = None,
    *,
    league: str | None = None,
    regulation_periods: int | None = None,
    cells: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Expected points by zone and situation for both teams of one game (works mid-game).

    Counts, situations, zones and the expected-point measures the module docstring defines: the
    BB-D9 expectation on every field-goal attempt of the plays so far, summed over BB-D31's
    possessions and BB-D35's zones.

    Args:
        pbp: One game's plays in the basketball pbp schema, after
            :func:`~sportsdataverse.nba.nba_game_page.shot_coordinates`, in play order (as
            :func:`~sportsdataverse.nba.nba_game_splits.team_game_splits` reads them).
        player_box: Unused; accepted so the call matches the BB-D31 splits.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 or 4); default
            :func:`~sportsdataverse.nba.nba_game_page.game_regulation_periods` from ``season``.
        cells: :func:`shot_cells` of the league as of the game date; ``None`` leaves every
            expected column null (the counts and actual rates still fill).

    Returns:
        One row per team in :data:`TEAM_EXPECTED_SCHEMA`; an empty frame with those columns before
        tip-off. An expected sum is null when any of its attempts has no expectation.

    Raises:
        ValueError: If the league is unknown or ``pbp`` holds more than one game.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.wnba.wnba_pbp import espn_wnba_pbp
            frames = game_page_frames(espn_wnba_pbp(401736188, return_parsed=False), league="wnba", shot_cells=cells)
            frames["team_game_expected"].select("team_id", "xfg_pts", "poe", "xts_pct", "xdrtg")

        See Also:
            * :func:`shot_cells` / :func:`shot_xfg` -- the per-shot expectation.
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    del player_box  # ponytail: same call shape as the D31 splits; no box column is read
    lg = _resolve_league(league, None)
    if pbp.height == 0:
        return pl.DataFrame(schema=TEAM_EXPECTED_SCHEMA)
    if pbp["game_id"].n_unique() > 1:
        raise ValueError(f"team_game_expected reads one game; got {pbp['game_id'].n_unique()} game_id values")
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=_box_season(pbp))
    rows = _split_events(pbp)
    out = _expected_rows(rows, _possessions(rows, lg, reg), _scored_shots(rows, lg, cells))
    poe = pl.col("fg_pts") - pl.col("xfg_pts")
    tsa = 2.0 * (pl.col("fga") + 0.44 * pl.col("fta"))
    out = out.with_columns(
        poe.alias("poe"),
        _ratio(poe, pl.col("fga")).alias("poe_per_shot"),
        _ratio(pl.col("fgm") + 0.5 * pl.col("fg3m"), pl.col("fga")).alias("efg_pct"),
        _ratio(pl.col("xfg_pts"), 2 * pl.col("fga")).alias("xefg_pct"),
        _ratio(pl.col("points"), tsa).alias("ts_pct"),
        _ratio(pl.col("xfg_pts") + pl.col("ftm"), tsa).alias("xts_pct"),
        (100 * _ratio(pl.col("points"), pl.col("possessions"))).alias("ortg"),
        (100 * _ratio(pl.col("points") - poe, pl.col("possessions"))).alias("xortg"),
        *[_ratio(pl.col(f"{s}_poss"), pl.col("possessions")).alias(f"{s}_rate") for s in SITUATIONS],
        *[_ratio(pl.col(f"{s}_pts"), pl.col(f"{s}_poss")).alias(f"{s}_ppp") for s in SITUATIONS],
        *[_ratio(pl.col(f"_{z}_pts"), 2 * pl.col(f"{z}_fga")).alias(f"{z}_efg_pct") for z in ZONES],
        *[_ratio(pl.col(f"_{z}_xpts"), pl.col(f"{z}_fga")).alias(f"{z}_xppp") for z in ZONES],
        *[(pl.col(f"_{z}_pts") - pl.col(f"_{z}_xpts")).alias(f"{z}_poe") for z in ZONES],
    )
    # each row's opponent is the other row: a two-row reverse
    out = out.with_columns(pl.col("ortg").reverse().alias("drtg"), pl.col("xortg").reverse().alias("xdrtg"))
    if cells is None:  # an empty zone or situation would otherwise read 0 - 0.0 = 0.0 over expected
        out = out.with_columns(pl.lit(None, pl.Float64).alias(c) for c in _EXPECTED_COLS)
    return out.select([pl.col(c).cast(t) for c, t in TEAM_EXPECTED_SCHEMA.items()])
