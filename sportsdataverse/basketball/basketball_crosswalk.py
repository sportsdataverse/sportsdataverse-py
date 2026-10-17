"""Deterministic id crosswalks between the international basketball families.

Every table here joins ids that two providers already share on the wire; nothing is guessed from
names. Each helper reads a frame you already have from a wrapper (so no request is made), checks the
id dtypes, and returns the distinct id pairs.

**Key spaces.**

* ``genius_match_id`` (``Int64``): the Genius Sports LiveStats match id, the key of the ``genius``
  family (``match_id`` of ``genius_game``, ``genius_game_box`` ...). Providers that relay Genius carry
  it under their own column name; :func:`genius_game_crosswalk` maps their game key to it.
* ``genius_team_id`` (``Int64``): the Genius team id. Only the Sportality sites (Basketligaen) carry it
  next to the platform team uuid. The ``genius`` schedule frames' ``home_team_id`` / ``away_team_id``
  are the same space.
* Genius **person** ids have no second carrier except the Sportality platform:
  ``sportality_player_crosswalk`` (Genius person id <-> athlete uuid) already is that table. CEBL's,
  TBF's and the other families' ``player_id`` are their own ids, not Genius'.
* Other families need no table: NBL1 ``nbl_rosetta`` ``team_id`` / ``player_id`` / ``match_id`` are the
  DataCore ids ``atrium`` calls ``entity_id`` / ``person_id`` / ``fixture_id``, so those frames join
  directly (string uuids on both sides).

Example:
    Quick start::

        from sportsdataverse.basketball import bnxt
        from sportsdataverse.basketball.basketball_crosswalk import genius_game_crosswalk

        slider = bnxt.sportpress_game_slider(competition_id=...)
        xw = genius_game_crosswalk(slider, source="sportpress")
        # columns: game_id (Int64, BNXT), genius_match_id (Int64)

    See Also:
        * :mod:`sportsdataverse.basketball.genius` -- the Genius LiveStats family the ids point to
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import polars as pl

__all__ = [
    "GENIUS_GAME_SOURCES",
    "GENIUS_TEAM_SOURCES",
    "genius_game_crosswalk",
    "genius_team_crosswalk",
    "ibba_segev_game_crosswalk",
    "pleague_team_crosswalk",
]

#: ``source -> (provider id column, provider id dtype, Genius match id column)``.
#: ``sportpress`` = ``sportpress_game_slider``; ``cebl`` = ``cebl_games`` / ``cebl_player_gamelog``;
#: ``tbf`` = ``tbf_schedule`` / ``tbf_daily_matches`` / ``tbf_game_header``; ``kzs`` = ``kzs_matches`` /
#: ``kzs_match``; ``sportality`` = ``sportality_pbp`` / ``sportality_team_stats`` (the platform game uuid and
#: the Genius match id ``game_id`` sit on the same row).
GENIUS_GAME_SOURCES: Dict[str, Tuple[str, Any, str]] = {
    "sportpress": ("game_id", pl.Int64, "genius_id"),
    "cebl": ("game_id", pl.Int64, "genius_match_id"),
    "tbf": ("match_id", pl.Int64, "genius_id"),
    "kzs": ("match_id", pl.Int64, "genius_match_id"),
    "sportality": ("game_uuid", pl.Utf8, "game_id"),
}

#: ``source -> (platform team uuid column, Genius team id column)`` on Sportality frames.
GENIUS_TEAM_SOURCES: Dict[str, Tuple[str, str]] = {
    "sportality_standings": ("team_uuid", "team_id"),
    "sportality_teams": ("info_uuid", "info_team_id"),
    "sportality_players": ("info_team_uuid", "info_team_id"),
}

# site team_id -> Preciser team uuid (the club-logo file name on the standings page). Verified against the
# standings fixture and the gid-197 shot list; the same table is in the pleague_preciser notes.
_PLG_TEAMS = (
    (1, "e86394aa-3d88-11ed-8ba8-7f44ba46b0b1"),
    (2, "e865bb20-3d88-11ed-a956-7f44ba46b0b1"),
    (3, "e8674343-3d88-11ed-98b4-7f44ba46b0b1"),
    (4, "e869743f-3d88-11ed-925e-7f44ba46b0b1"),
    (5, "e86cac11-3d88-11ed-8637-7f44ba46b0b1"),
    (6, "e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1"),
    (7, "3c3a601a-358a-11ed-ab92-711c34ed1298"),
    (8, "bf957f5a-9389-11f0-812c-a38482ec4d30"),
)


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Any:
    return df.to_pandas() if return_as_pandas else df


def _pairs(df: Any, left: str, right: str, dtypes: Tuple[Any, Any], label: str) -> pl.DataFrame:
    """Distinct non-null ``(left, right)`` pairs; dtypes asserted; one-to-one both ways."""
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"{label}: expected a polars DataFrame, got {type(df).__name__}")
    missing = [c for c in (left, right) if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: frame has no column {missing} (columns: {df.columns})")
    for col, want in zip((left, right), dtypes):
        if df.schema[col] != want:
            raise TypeError(f"{label}: {col} is {df.schema[col]}, expected {want} (no float or string id casts)")
    out = df.select(left, right).drop_nulls().unique(maintain_order=True)
    for a, b in ((left, right), (right, left)):
        dup = out.group_by(a).agg(pl.col(b).n_unique().alias("n")).filter(pl.col("n") > 1)
        if dup.height:
            raise ValueError(f"{label}: {a} maps to more than one {b}: {dup[a].head(3).to_list()}")
    return out


def genius_game_crosswalk(df: pl.DataFrame, source: str, *, return_as_pandas: bool = False) -> Any:
    """Provider game key <-> Genius LiveStats match id, from a frame that carries both.

    Args:
        df (polars.DataFrame): a frame from the wrapper named in :data:`GENIUS_GAME_SOURCES` for ``source``.
        source (str): ``sportpress`` (BNXT ``sportpress_game_slider``: ``game_id`` / ``genius_id``), ``cebl``
            (``game_id`` / ``genius_match_id``), ``tbf`` (``match_id`` / ``genius_id``), ``kzs`` (``match_id`` /
            ``genius_match_id``) or ``sportality`` (``game_uuid`` / ``game_id``, the Genius match id).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per game with ``game_key`` (the provider's key: ``Int64``,
        or the platform game uuid ``Utf8`` for ``sportality``) and ``genius_match_id`` (``Int64``). Rows
        without a Genius id (older seasons, upcoming games) are dropped.

    Raises:
        ValueError: unknown ``source``, a missing column, or a key that maps to two ids.
        TypeError: ``df`` is not polars, or an id column has the wrong dtype.

    Example:
        Quick start::

            from sportsdataverse.basketball.basketball_crosswalk import genius_game_crosswalk
            xw = genius_game_crosswalk(cebl.cebl_games(season=2025), "cebl")
            games = xw.join(genius.genius_competition_games(...), left_on="genius_match_id", right_on="match_id")
    """
    if source not in GENIUS_GAME_SOURCES:
        raise ValueError(f"unknown source {source!r}; choose from {sorted(GENIUS_GAME_SOURCES)}")
    key, key_dtype, gcol = GENIUS_GAME_SOURCES[source]
    out = _pairs(df, key, gcol, (key_dtype, pl.Int64), f"genius_game_crosswalk[{source}]")
    return _out(out.rename({key: "game_key", gcol: "genius_match_id"}), return_as_pandas)


def genius_team_crosswalk(df: pl.DataFrame, source: str, *, return_as_pandas: bool = False) -> Any:
    """Sportality platform team uuid <-> Genius team id, from a Sportality frame that carries both.

    Args:
        df (polars.DataFrame): a Sportality frame.
        source (str): ``sportality_standings`` (``sportality_standings``: ``team_uuid`` / ``team_id``),
            ``sportality_teams`` (``sportality_stats_info`` ``teams_summary``: ``info_uuid`` / ``info_team_id``) or
            ``sportality_players`` (``players_summary``: ``info_team_uuid`` / ``info_team_id``).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: ``team_uuid`` (``Utf8``) and ``genius_team_id`` (``Int64``), one row
        per team. The Genius ids are the ones the ``genius`` schedule frames call ``home_team_id``.

    Raises:
        ValueError: unknown ``source``, a missing column, or a key that maps to two ids.
        TypeError: ``df`` is not polars, or an id column has the wrong dtype.

    Example:
        Quick start::

            from sportsdataverse.basketball.basketball_crosswalk import genius_team_crosswalk
            xw = genius_team_crosswalk(standings, "sportality_standings")
    """
    if source not in GENIUS_TEAM_SOURCES:
        raise ValueError(f"unknown source {source!r}; choose from {sorted(GENIUS_TEAM_SOURCES)}")
    ucol, gcol = GENIUS_TEAM_SOURCES[source]
    out = _pairs(df, ucol, gcol, (pl.Utf8, pl.Int64), f"genius_team_crosswalk[{source}]")
    return _out(out.rename({ucol: "team_uuid", gcol: "genius_team_id"}), return_as_pandas)


def ibba_segev_game_crosswalk(df: pl.DataFrame, *, return_as_pandas: bool = False) -> Any:
    """IBBA ``game_id`` <-> Segev ``game_id`` from ``ibba_games`` (the ``segev_game_id`` column).

    Args:
        df (polars.DataFrame): an ``ibba_games`` frame (2025-26 on; earlier rows and external-team
            news links carry no Segev id).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: ``game_id`` (IBBA) and ``segev_game_id`` (the ``game_id`` every
        ``ibba_segev_*`` wrapper takes), both ``Int64``.

    Raises:
        ValueError: a missing column, or a key that maps to two ids.
        TypeError: ``df`` is not polars, or an id column has the wrong dtype.

    Example:
        Quick start::

            from sportsdataverse.basketball.basketball_crosswalk import ibba_segev_game_crosswalk
            xw = ibba_segev_game_crosswalk(ibba.ibba_games(season=2026))
    """
    out = _pairs(df, "game_id", "segev_game_id", (pl.Int64, pl.Int64), "ibba_segev_game_crosswalk")
    return _out(out, return_as_pandas)


def pleague_team_crosswalk(*, return_as_pandas: bool = False) -> Any:
    """P. LEAGUE+ site ``team_id`` (1-8) <-> Preciser team uuid, for every club ever listed.

    The site's ``team_id`` is stable across seasons and renames; the vendor keys teams by uuid
    (``team_uuid`` / ``home_team_uuid`` / ``away_team_uuid`` of the ``pleague_preciser`` frames). Persons
    need no table: the Preciser ``person_id`` is the site box row's ``person_id`` (same string).

    Args:
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: eight rows, ``team_id`` (``Int64``) and ``team_uuid`` (``Utf8``).

    Raises:
        None.

    Example:
        Quick start::

            from sportsdataverse.basketball.basketball_crosswalk import pleague_team_crosswalk
            shots.join(pleague_team_crosswalk(), on="team_uuid")
    """
    out = pl.DataFrame(_PLG_TEAMS, schema={"team_id": pl.Int64, "team_uuid": pl.Utf8}, orient="row")
    return _out(out, return_as_pandas)
