"""Vintage-keyed feature store: the structural leakage guard for the bake-off.

Every feature table is keyed by ``(season, as_of_week, entity)`` where a row
at ``as_of_week == W`` was built from games in weeks **strictly before W**
(EXCLUSIVE convention). CFB ``through_week`` assets (INCLUSIVE of their
week) must be shifted at ingest: ``as_of_week = through_week + 1``.

The backtest driver joins features ONLY through this store —
:meth:`VintageStore.register` refuses undated frames, and
:meth:`VintageStore.join_asof` joins a week-``W`` game to the latest vintage
with ``as_of_week <= W`` (backward fill over missing snapshots is leak-free;
future vintages are structurally unreachable).
"""

from __future__ import annotations

import warnings
from typing import Literal

import polars as pl

__all__ = ["VINTAGE_KEYS", "VintageStore"]

VINTAGE_KEYS = ("season", "as_of_week")


class VintageStore:
    """In-memory registry of vintage-keyed feature frames.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.wexp.store import VintageStore
            store = VintageStore()
            store.register("ratings", ratings_frame, entity_key="team_id")
            games = store.join_asof(games, "ratings",
                                    on={"home_team_id": "team_id"},
                                    prefix="home_")
    """

    def __init__(self) -> None:
        self._tables: dict[str, tuple[pl.DataFrame, str]] = {}

    def register(
        self,
        name: str,
        frame: pl.DataFrame,
        *,
        entity_key: str,
        week_semantics: Literal["as_of", "through"] = "as_of",
    ) -> None:
        """Register a feature frame; refuse anything not vintage-keyed.

        Args:
            name: Table name.
            frame: Frame carrying vintage keys + ``entity_key``. With
                ``week_semantics="as_of"`` the frame has an ``as_of_week``
                column whose row ``W`` was built from weeks strictly before
                ``W``. With ``week_semantics="through"`` the frame has a
                ``through_week`` column INCLUSIVE of its week (the CFB
                ``cfb_ratings_weekly`` / ``cfb_team_summaries_weekly``
                convention) and the store performs the
                ``as_of_week = through_week + 1`` shift itself — so
                reintroducing the inclusive-week leak requires a deliberate
                wrong argument, not a forgotten ingest comment.
            entity_key: The entity column (e.g. ``"team_id"``).
            week_semantics: ``"as_of"`` (default) or ``"through"``.

        Raises:
            ValueError: If a vintage key or the entity column is missing, or
                ``(season, as_of_week, entity)`` rows are duplicated.
        """
        if week_semantics == "through":
            if "through_week" not in frame.columns:
                raise ValueError(f"table {name!r}: week_semantics='through' requires a 'through_week' column")
            frame = frame.rename({"through_week": "as_of_week"}).with_columns(
                (pl.col("as_of_week") + 1).alias("as_of_week")
            )
        missing = [k for k in (*VINTAGE_KEYS, entity_key) if k not in frame.columns]
        if missing:
            raise ValueError(
                f"table {name!r} is not vintage-keyed: missing column(s) {missing}; "
                f"every feature table needs {VINTAGE_KEYS} + entity"
            )
        key_cols = [*VINTAGE_KEYS, entity_key]
        n_dup = frame.height - frame.unique(subset=key_cols).height
        if n_dup:
            raise ValueError(f"table {name!r} has {n_dup} duplicate row(s) on {key_cols}")
        self._tables[name] = (frame, entity_key)

    def table(self, name: str) -> pl.DataFrame:
        """Return a registered frame.

        Args:
            name: Table name passed to :meth:`register`.

        Returns:
            The registered ``polars.DataFrame``.
        """
        return self._tables[name][0]

    def join_asof(
        self,
        games: pl.DataFrame,
        name: str,
        *,
        on: dict[str, str],
        prefix: str = "",
        week_col: str = "week",
    ) -> pl.DataFrame:
        """Join a games frame to the latest leak-free vintage of a table.

        A game in week ``W`` receives the row with the greatest
        ``as_of_week <= W`` for its entity (backward fill across missing
        snapshots). Games earlier than every vintage get nulls — never a
        future value.

        Args:
            games: Frame with ``season``, ``week_col``, and the left join key.
            name: Registered table name.
            on: Single-entry mapping ``{games_key: table_entity_key}``.
            prefix: Prefix applied to the joined feature columns.
            week_col: Week column on ``games``.

        Returns:
            ``games`` with the vintage's feature columns appended, plus
            ``{prefix}as_of_week`` recording which vintage served each game
            (leak forensics: it is always ``<=`` the game's week).

        Raises:
            KeyError: If ``name`` is not registered.
            ValueError: If join-key dtypes disagree, or a joined feature
                column would collide with an existing games column.
        """
        if name not in self._tables:
            raise KeyError(f"table {name!r} not registered")
        frame, entity_key = self._tables[name]
        (games_key, table_key), *rest = on.items()
        if rest or table_key != entity_key:
            raise ValueError(f"on must map one games column to {entity_key!r}")
        if games.schema[games_key] != frame.schema[entity_key]:
            raise ValueError(
                f"join-key dtype mismatch: games[{games_key!r}]={games.schema[games_key]} "
                f"vs {name}[{entity_key!r}]={frame.schema[entity_key]}"
            )
        if games.schema[week_col] != frame.schema["as_of_week"]:
            raise ValueError(
                f"join-key dtype mismatch: games[{week_col!r}]={games.schema[week_col]} "
                f"vs {name}[as_of_week]={frame.schema['as_of_week']}"
            )
        if games.schema["season"] != frame.schema["season"]:
            raise ValueError(
                f"join-key dtype mismatch: games[season]={games.schema['season']} "
                f"vs {name}[season]={frame.schema['season']}"
            )
        feature_cols = [c for c in frame.columns if c not in (*VINTAGE_KEYS, entity_key)]
        out_names = [f"{prefix}{c}" for c in feature_cols] + [f"{prefix}as_of_week"]
        collisions = sorted(set(out_names) & set(games.columns))
        if collisions:
            raise ValueError(
                f"joined column(s) {collisions} collide with existing games columns — pass a distinct prefix"
            )
        right = frame.select(
            pl.col("season"),
            pl.col("as_of_week"),
            pl.col(entity_key).alias(games_key),
            *[pl.col(c).alias(f"{prefix}{c}") for c in feature_cols],
        ).sort("as_of_week")
        out = games.with_row_index("__order").sort(week_col)
        with warnings.catch_warnings():
            # polars cannot verify per-group sortedness with `by`; both sides
            # are sorted on the asof key above. Scope the filter to that one
            # message — do not swallow unrelated UserWarnings.
            warnings.filterwarnings("ignore", message="Sortedness", category=UserWarning)
            out = out.join_asof(
                right,
                left_on=week_col,
                right_on="as_of_week",
                by=["season", games_key],
                strategy="backward",
            )
        # keep the serving vintage as {prefix}as_of_week (leak forensics)
        rename = {"as_of_week": f"{prefix}as_of_week"} if prefix else {}
        return out.sort("__order").drop("__order").rename(rename)
