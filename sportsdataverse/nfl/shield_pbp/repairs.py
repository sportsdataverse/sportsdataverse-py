"""Hardcoded per-game repairs, ported from nflfastR's ``helper_scrape_nfl.R``.

Port of reference doc §1 (``fix_bad_games`` lines 358-421 and ``fix_posteams``
lines 423-462). In nflfastR both run inside ``get_pbp_nfl`` on a *single* game's
frame: ``fix_posteams()`` runs unconditionally first, then ``fix_bad_games()``
runs only when that game's raw feed had ``home_team == away_team`` (a scrape
bug flagged as ``bad_game <- 1`` at parse time — not a fixed list of game_ids;
it is re-derived at runtime from the frame's own ``home_team``/``away_team``
columns, which is exactly how :func:`apply_game_repairs` gates it here so it
stays safe to call on a concatenated multi-game frame).

Column availability at this pipeline stage (called immediately after
:func:`sportsdataverse.nfl.shield_pbp.parse.parse_game`, before description/features/labels):

* ``fix_bad_games`` mutates four columns in nflfastR: ``td_team``,
  ``return_team``, ``fumble_recovery_1_team``, ``timeout_team``. Of those,
  **``td_team`` is deferred** — both of its R branches key on
  ``drive_how_ended_description``, a column the native Shield parser never
  produces at (or after) this stage, so there is no way to distinguish the
  offensive-TD case from the defensive-TD case without guessing. The other
  three columns (``return_team``, ``fumble_recovery_1_team``,
  ``timeout_team``) are fully ported. ``timeout_team``'s R source column is
  ``play_description``; the native frame's equivalent is ``desc`` (see
  ``sportsdataverse/nfl/shield_pbp/parse.py``), so the regex is applied against ``desc``.
* ``fix_posteams`` re-derives ``posteam`` from a ``pre_play_by_play`` column
  (a ``"KC  1-10  NYJ 40"``-shaped narrative string) when present, else it is
  a no-op — literally, per the R source's own defensive
  ``if ("pre_play_by_play" %in% names(pbp))`` gate. The native Shield pipeline
  never emits a ``pre_play_by_play`` column (that field belongs to the older
  NFL GameCenter narrative-log scrape format ``helper_scrape_nfl.R`` targets,
  not the modern Shield API payload the native parser reads), so this port
  only implements the defensive gate; the regex-extraction branch is
  **deferred** — it is unreachable dead code for this pipeline, and faithfully
  reproducing the R regex's alternation-precedence quirk (the ``^`` anchor and
  the trailing lookahead each bind to only one alternative in the team-abbr
  disjunction, not all of them) with no real ``pre_play_by_play`` fixture to
  validate against would be guessing rather than porting.

Port of reference doc §2 (``helper_add_nflscrapr_mutations.R :: fix_scrambles``,
lines 797-818) and §3 (``helper_additional_functions.R :: fix_weird_pass_plays``,
lines 641-661):

* ``fix_scrambles`` backfills ``qb_scramble`` to ``1`` for the 1999-2005
  charting-data-identified scrambles the NFL's raw feed didn't mark in the
  play description (see :data:`_SCRAMBLE_FIX_PATH` / ``data/README.md`` for
  provenance of the vendored 5,830-row key list). No-ops on frames whose
  minimum ``season`` is after 2005 (matching the R source's own early
  ``return(pbp)``), and defensively no-ops when ``season`` / ``game_id`` /
  ``play_id`` / ``qb_scramble`` aren't all present.
* ``fix_weird_pass_plays`` force-zeroes a hardcoded 15-row ``pass`` false-
  positive list (garbled play descriptions nflfastR's own regex-based ``pass``
  classifier misclassified). The native pipeline derives the ``pass``/``rush``
  0/1 classification columns in :func:`sportsdataverse.nfl.shield_pbp.description.add_pass_rush`
  (a faithful port of nflfastR ``clean_pbp``'s derivation, reference §6),
  which applies this override at nflfastR's exact position — after the
  ``pass`` derivation and BEFORE the ``rush`` derivation, so ``rush`` is
  computed from the fixed ``pass`` value. The function still gates gracefully
  (no-ops) when the ``pass`` column is absent, so it stays safe to call on a
  pre-classification frame.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

# Columns fix_bad_games can actually repair with what the native frame carries
# at this pipeline stage (td_team is deferred — see module docstring).
_BAD_GAME_REPAIR_COLUMNS = ("return_team", "fumble_recovery_1_team", "timeout_team")

# Vendored nflfastR sysdata.rda `scramble_fix` list (5,830 "{game_id}_{play_id}"
# keys) — see data/README.md for provenance. Loaded once and cached.
_SCRAMBLE_FIX_PATH = Path(__file__).parent / "data" / "scramble_fix.csv"

# Verbatim transcription of helper_additional_functions.R's fix_weird_pass_plays
# false_positives list (15 rows) — pre-2006-through-2020 hardcoded garbled plays
# where the regex-based pass classifier reached its limit. Never guessed at;
# every key here is copied character-for-character from the R source.
_WEIRD_PASS_FALSE_POSITIVES: frozenset[str] = frozenset(
    {
        "1999_01_ARI_PHI_1611",
        "1999_01_SF_JAX_1788",
        "1999_01_SF_JAX_2081",
        "1999_11_ATL_TB_1740",
        "2001_09_MIN_PHI_1307",
        "2001_14_NE_BUF_452",
        "2002_16_PIT_TB_527",
        "2003_02_HOU_NO_3924",
        "2003_15_PIT_NYJ_873",
        "2004_05_BUF_NYJ_2555",
        "2005_07_SD_PHI_321",
        "2011_02_STL_NYG_1369",
        "2016_05_NE_CLE_912",
        "2016_06_CAR_NO_2690",
        "2020_10_BAL_NE_2013",
    }
)


@lru_cache(maxsize=1)
def _load_scramble_fix() -> frozenset[str]:
    """Module-level cached load of the vendored ``scramble_fix`` key set.

    Returns:
        A ``frozenset`` of 5,830 ``"{game_id}_{play_id}"`` keys. Cached after
        first call (``lru_cache``) so repeated invocations across many games
        in a season build don't re-read the CSV from disk.
    """
    return frozenset(pl.read_csv(_SCRAMBLE_FIX_PATH)["scramble_id"].to_list())


def _play_key_expr() -> pl.Expr:
    """``"{game_id}_{play_id}"`` key expression matching nflfastR's paste0 keys.

    ``play_id`` is routed through a NON-strict ``Int64`` cast so every dtype
    the native frame ships renders the way nflfastR's numeric play_id prints:
    floats drop the ``.0`` (``1611.0`` -> ``"1611"``), numeric strings pass
    through (``"1611"`` -> ``"1611"``), and non-numeric strings (synthetic
    test ids like ``"p1"``) become null — a null key matches nothing and the
    row falls through untouched instead of raising. The non-strict cast also
    truncates fractional floats (``1611.7`` -> ``"1611"``); theoretical only,
    since GSIS play ids are integral.
    """
    return pl.concat_str(
        [pl.col("game_id"), pl.col("play_id").cast(pl.Int64, strict=False).cast(pl.Utf8)],
        separator="_",
    )


def fix_posteams(df: pl.DataFrame) -> pl.DataFrame:
    """Re-derive ``posteam`` from a ``pre_play_by_play`` narrative column.

    No-ops (returns ``df`` unchanged) when ``pre_play_by_play`` is absent —
    always true for the native Shield pipeline. See the module docstring for
    why the present-column branch is deferred rather than implemented.

    Args:
        df: A play-level frame, one or more games.

    Returns:
        ``df`` unchanged (the only reachable path for this port).
    """
    if "pre_play_by_play" not in df.columns:
        return df
    # Deferred — see module docstring. Left as a no-op rather than guessed at.
    return df


def fix_bad_games(df: pl.DataFrame) -> pl.DataFrame:
    """Row-wise corrections for games where the raw feed had ``home_team == away_team``.

    Faithful (partial — see module docstring) port of nflfastR's
    ``fix_bad_games``. Applies its mutation to every row passed in
    unconditionally (matching the R function, which itself does not check the
    ``bad_game`` condition — the caller does); :func:`apply_game_repairs` is
    what scopes the correction to only the affected game's rows on a
    multi-game frame.

    Args:
        df: A play-level frame carrying (a subset of) ``posteam``,
            ``home_team``, ``away_team``, ``fumble_lost``, ``return_team``,
            ``fumble_recovery_1_team``, ``timeout_team``, ``desc``.

    Returns:
        ``df`` with any of ``return_team`` / ``fumble_recovery_1_team`` /
        ``timeout_team`` present recomputed; columns absent from ``df`` (or
        missing a required dependency) are left untouched. ``td_team`` is
        never mutated (deferred).
    """
    exprs: list[pl.Expr] = []
    cols = set(df.columns)

    if {"return_team", "posteam", "home_team", "away_team"} <= cols:
        exprs.append(
            pl.when(pl.col("return_team").is_not_null())
            .then(
                pl.when(pl.col("posteam") == pl.col("home_team"))
                .then(pl.col("away_team"))
                .otherwise(pl.col("home_team"))
            )
            .otherwise(pl.col("return_team").cast(pl.Utf8))
            .alias("return_team")
        )

    if {"fumble_recovery_1_team", "fumble_lost", "posteam", "home_team", "away_team"} <= cols:
        # Games with zero fumbles leave this column 100%-null, which polars infers
        # as dtype Null (not Utf8) — cast explicitly so the when/otherwise chain
        # doesn't hit a Null/String supertype mismatch at collect time.
        exprs.append(
            pl.when(pl.col("fumble_recovery_1_team").is_not_null())
            .then(
                pl.when((pl.col("fumble_lost") == 1) & (pl.col("posteam") == pl.col("home_team")))
                .then(pl.col("away_team"))
                .when((pl.col("fumble_lost") == 1) & (pl.col("posteam") == pl.col("away_team")))
                .then(pl.col("home_team"))
                .when((pl.col("fumble_lost") == 0) & (pl.col("posteam") == pl.col("home_team")))
                .then(pl.col("home_team"))
                .when((pl.col("fumble_lost") == 0) & (pl.col("posteam") == pl.col("away_team")))
                .then(pl.col("away_team"))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
            )
            .otherwise(pl.col("fumble_recovery_1_team").cast(pl.Utf8))
            .alias("fumble_recovery_1_team")
        )

    if {"timeout_team", "desc"} <= cols:
        exprs.append(
            pl.when(pl.col("timeout_team").is_not_null())
            .then(pl.col("desc").cast(pl.Utf8).str.extract(r"Timeout #[1-3] by ([A-Z]+)", 1))
            .otherwise(pl.col("timeout_team").cast(pl.Utf8))
            .alias("timeout_team")
        )

    if not exprs:
        return df
    return df.with_columns(exprs)


def apply_game_repairs(df: pl.DataFrame) -> pl.DataFrame:
    """Apply :func:`fix_posteams` then the gated :func:`fix_bad_games` repair.

    Idempotent and safe on a concatenated multi-game frame: the
    ``home_team == away_team`` "bad game" condition is derived per-row (these
    two columns are already game-constant, set once per game by
    :func:`sportsdataverse.nfl.shield_pbp.parse.parse_game`), so only the affected game's rows are
    touched — every other row is returned byte-identical.

    Args:
        df: The post-``parse_game`` play frame (one or many games).

    Returns:
        ``df`` with ``posteam`` (via :func:`fix_posteams`, currently always a
        no-op — see module docstring) and, for any game whose rows show
        ``home_team == away_team``, ``return_team`` /
        ``fumble_recovery_1_team`` / ``timeout_team`` repaired.
    """
    if df.height == 0:
        return df

    df = fix_posteams(df)

    if not {"home_team", "away_team"} <= set(df.columns):
        return df
    repair_cols = [c for c in _BAD_GAME_REPAIR_COLUMNS if c in df.columns]
    if not repair_cols:
        return df

    bad_game = (
        pl.col("home_team").is_not_null()
        & pl.col("away_team").is_not_null()
        & (pl.col("home_team") == pl.col("away_team"))
    )
    fixed = fix_bad_games(df)
    return df.with_columns([pl.when(bad_game).then(fixed[c]).otherwise(pl.col(c)).alias(c) for c in repair_cols])


def fix_scrambles(df: pl.DataFrame) -> pl.DataFrame:
    """Backfill ``qb_scramble`` for 1999-2005 plays the raw feed didn't mark.

    Faithful port of ``helper_add_nflscrapr_mutations.R :: fix_scrambles``
    (reference §2). Builds the row key ``f"{game_id}_{play_id}"`` and flips
    ``qb_scramble`` to ``1`` wherever that key is in the vendored
    :data:`_SCRAMBLE_FIX_PATH` set; never flips an existing ``1`` back to
    ``0``, and never touches ``play_type`` / ``qb_dropback`` (those are
    derived from the *un-fixed* ``qb_scramble`` earlier in the pipeline — see
    the module docstring's parity-critical ordering note).

    Args:
        df: A play-level frame carrying (a subset of) ``season``, ``game_id``,
            ``play_id``, ``qb_scramble``.

    Returns:
        ``df`` unchanged if empty, if ``min(season) > 2005`` (mirrors the R
        source's early return), or if any of the four required columns are
        absent. Otherwise ``df`` with ``qb_scramble`` backfilled.
    """
    if df.height == 0:
        return df

    required = {"season", "game_id", "play_id", "qb_scramble"}
    if not required <= set(df.columns):
        return df

    min_season = df.select(pl.col("season").min()).item()
    if min_season is None or min_season > 2005:
        return df

    scramble_fix = _load_scramble_fix()
    return df.with_columns(
        qb_scramble=pl.when(_play_key_expr().is_in(scramble_fix)).then(1).otherwise(pl.col("qb_scramble"))
    )


def fix_weird_pass_plays(df: pl.DataFrame) -> pl.DataFrame:
    """Force-zero the 15 hardcoded ``pass`` false-positive plays (reference §3).

    Faithful port of ``helper_additional_functions.R :: fix_weird_pass_plays``.
    Only ever forces ``pass`` from ``1`` to ``0`` on the fixed
    :data:`_WEIRD_PASS_FALSE_POSITIVES` game_id/play_id list; never sets
    ``pass`` to ``1``. Called by :func:`sportsdataverse.nfl.shield_pbp.description.add_pass_rush`
    between its ``pass`` and ``rush`` derivations (nflfastR's exact position —
    ``rush`` must be computed from the fixed ``pass``).

    Args:
        df: A play-level frame carrying (a subset of) ``game_id``,
            ``play_id``, ``pass``.

    Returns:
        ``df`` with ``pass`` zeroed on the matching rows; unchanged if empty
        or if ``pass`` / ``game_id`` / ``play_id`` aren't all present (a
        defensive gate for pre-classification frames).
    """
    if df.height == 0:
        return df

    required = {"game_id", "play_id", "pass"}
    if not required <= set(df.columns):
        return df

    return df.with_columns(
        **{"pass": pl.when(_play_key_expr().is_in(_WEIRD_PASS_FALSE_POSITIVES)).then(0).otherwise(pl.col("pass"))}
    )
