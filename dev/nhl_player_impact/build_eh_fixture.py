"""Turn the raw Evolving Hockey CSV exports (downloaded by ``eh_capture.py`` into
``dev/nhl_player_impact/_cache/``, gitignored) into the committed
``tests/fixtures/nhl_player_impact/eh_skaters.parquet`` concurrent-validity oracle
fixture for the skater xG-RAPM (Phase 3) and GAR/WAR (Phase 6) gates in
``test_nhl_player_impact_oracle.py``.

EH↔NHL player-id crosswalk
--------------------------
Evolving Hockey's CSV exports carry no NHL ``playerId`` column (unlike MoneyPuck) --
only a display ``Player`` name. This script therefore builds the ``player_id`` column
by a **name-based crosswalk against sportsdataverse's OWN internal player universe**
(the skaters who appear in the committed ``shifts_sample.parquet``'s
``ids_on``/``players_on`` + ``ids_off``/``players_off`` parallel comma-joined lists,
which cover every skater on the ice for the 3 captured games) -- NOT a general
EH-wide id crosswalk. ``eh_skaters.parquet`` is therefore intentionally scoped to the
~72 skaters who both (a) appear in the 3-game internal fixture and (b) have a
case-fold-matched name in EH's 2024-25 export, mirroring how the internal
construction-invariant gates only ever exercise the 3-game sample. It is NOT a
general-purpose EH season table the way ``mp_gsax.parquet`` is a general-purpose
MoneyPuck season table (MoneyPuck carries the real NHL id; EH does not).

Known name-spelling divergences observed during capture (documented so a future
re-run doesn't re-litigate them): EH renders "Alexei Toropchenko" where sdv-py's NHL
API source has "Alexey Toropchenko" (transliteration variant) -- this player is
correctly dropped from the join rather than silently mismatched. Goalies are also
absent by construction (EH's skater tables exclude them; see ``mp_gsax.parquet`` for
the goalie-side oracle).

Strength-state definitions
---------------------------
EH's skater RAPM tool has no "All situations combined" option -- RAPM is inherently
strength-segmented (EV / PP / SH tables), unlike GAR/WAR which is always an
all-situations season total. So:

- ``xg_rapm`` here is EH's **EV** (even-strength) ``xG±/60``, TOI-weighted across any
  traded-player team-splits. The apples-to-apples internal comparator is
  ``nhl_skater_rapm(pbp, shifts, model_dir=..., strength_states=["5v5"])`` -- NOT the
  default all-situations call used by the internal ridge-centering gate.
- ``war`` here is EH's season-total ``WAR`` column (summed across any team-splits),
  directly comparable to the default (all-situations) ``nhl_skater_war(...)``.

Run with:
    uv run python dev/nhl_player_impact/build_eh_fixture.py
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

CACHE = Path(__file__).parent / "_cache"
REPO_ROOT = Path(__file__).resolve().parents[2]
FIX = REPO_ROOT / "tests" / "fixtures" / "nhl_player_impact"
OUT = FIX / "eh_skaters.parquet"

_EH_SKATERS_SCHEMA = {"player_id": pl.Int64, "player": pl.Utf8, "xg_rapm": pl.Float64, "war": pl.Float64}


def _internal_player_crosswalk(shifts: pl.DataFrame) -> dict[int, str]:
    """``player_id -> display name`` for every skater in the 3-game shifts fixture.

    Parses the parallel comma-space-joined ``ids_on``/``players_on`` and
    ``ids_off``/``players_off`` string columns (see the fixture README) -- there is no
    dedicated name column on the shifts frame itself.
    """
    mapping: dict[int, str] = {}
    for col_ids, col_names in (("ids_on", "players_on"), ("ids_off", "players_off")):
        sub = shifts.select(col_ids, col_names).drop_nulls()
        for ids_s, names_s in sub.iter_rows():
            if not ids_s or not names_s:
                continue
            ids = [x.strip() for x in ids_s.split(",")]
            names = [x.strip() for x in names_s.split(",")]
            if len(ids) != len(names):
                continue
            for raw_id, name in zip(ids, names):
                try:
                    mapping[int(raw_id)] = name
                except ValueError:
                    continue
    mapping.pop(0, None)
    return mapping


def _eh_rapm_ev(path: Path) -> pl.DataFrame:
    """TOI-weighted EV ``xG±/60`` per player, collapsing traded-player team-splits."""
    raw = pl.read_csv(path)
    return (
        raw.with_columns(pl.col("Player").str.to_lowercase().alias("name_key"))
        .group_by("name_key")
        .agg(
            xg_rapm=(pl.col("xG±/60") * pl.col("TOI")).sum() / pl.col("TOI").sum(),
        )
    )


def _eh_gar_war(path: Path) -> pl.DataFrame:
    """Season-total ``WAR`` per player, summing traded-player team-splits."""
    raw = pl.read_csv(path)
    return (
        raw.with_columns(pl.col("Player").str.to_lowercase().alias("name_key"))
        .group_by("name_key")
        .agg(war=pl.col("WAR").sum())
    )


def build() -> pl.DataFrame:
    shifts = pl.read_parquet(FIX / "shifts_sample.parquet")
    crosswalk = _internal_player_crosswalk(shifts)
    internal = pl.DataFrame({"player_id": list(crosswalk.keys()), "player": list(crosswalk.values())}).with_columns(
        name_key=pl.col("player").str.to_lowercase()
    )

    eh_rapm = _eh_rapm_ev(CACHE / "eh_skater_rapm_2024_regular_ev.csv")
    eh_war = _eh_gar_war(CACHE / "eh_skater_gar_2024_regular.csv")

    out = (
        internal.join(eh_rapm, on="name_key", how="inner")
        .join(eh_war, on="name_key", how="inner")
        .select("player_id", "player", "xg_rapm", "war")
        .sort("player_id")
    )
    assert out.schema == pl.Schema(_EH_SKATERS_SCHEMA), out.schema
    assert out["player_id"].n_unique() == out.height, "duplicate player_id after crosswalk join"
    return out


def main() -> None:
    out = build()
    print(f"EH skaters fixture: {out.height} matched skaters")
    out.write_parquet(OUT)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
