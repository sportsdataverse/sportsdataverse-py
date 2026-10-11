"""Per-game integrity screen for CFB play-by-play.

Box-score parity says a season drifted; it cannot say which games are broken
or why. This check derives one row per game of feed and processing failures:

  * ``unparsed`` -- more than half the plays carry no event flag at all
    (the game was never run through the play-type pipeline)
  * ``duplicate_rows`` -- a ``(game_id, id)`` key appears more than once
  * ``frozen_clock`` -- more than a fifth of the plays are stamped 0:00 (the
    2021-23 feed defect; a normal game has ~1%, the 99th percentile is 10%)
  * ``truncated`` -- the feed stops before the end of regulation: no fourth
    quarter, or a fourth quarter whose last clock reading is above 5:00 (a
    missing kneel-out tail ends near 2:30 and is not flagged)
  * ``down_skips`` -- two or more down jumps of 2+ (1st -> 3rd) inside one
    possession, i.e. plays missing from the feed
  * ``garbled_names`` -- regex-fallback player names that ran into the next
    clause ("Jeff Blanchardtackled b", "Smith to the Colo")

Each indicator is counted per season (games, or names for ``garbled_names``)
and compared with a committed ceiling measured from the current published
data, scaled by how many games the season has now against when it was
measured (an in-progress season grows every week); a count above the scaled
ceiling + tolerance is a WARN naming example games. The
hard failures (unparsed, duplicated, frozen clock) are also reported as INFO
whenever present, so the triage list never hides behind its own baseline.

    python -m tools.validation.checks.pbp_integrity --refresh cfb_pbp
    python -m tools.validation.checks.pbp_integrity --report cfb_pbp games.parquet
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.validation.findings import CheckContext, Finding, Severity

_FLOOR_DIR = Path(__file__).parent / "parity_floors"

c = pl.col

#: Event flags; a play with none of them is unparsed (timeouts and period
#: ends are, too, which is why the threshold is half the game).
_FLAGS = (
    "rush",
    "pass",
    "sack",
    "int",
    "punt_play",
    "fg_attempt",
    "kickoff_play",
    "penalty_flag",
    "fumble_vec",
    "safety",
    "kneel_down",
    "scrimmage_play",
    "td_play",
)
_NAME_COLUMNS = (
    "passer_player_name",
    "rusher_player_name",
    "receiver_player_name",
    "sack_player_name",
    "interception_player_name",
    "kickoff_return_player_name",
    "punt_return_player_name",
    "fumble_player_name",
    "fg_block_player_name",
    "punt_block_player_name",
)
_REQUIRED = (
    "game_id",
    "season",
    "id",
    "game_play_number",
    "period",
    "clock.displayValue",
    "drive.id",
    "pos_team_id",
    "start.down",
    *_FLAGS,
    *_NAME_COLUMNS,
)

#: A name holding a digit, a glued "tackled"/"returned", or a lowercase
#: football word ran into the next clause of the play text.
GARBLED_NAME = (
    r"\d|tackled|returned|\bfor\b|\bthe\b|\bto\b|\bat\b|\bby\b|\byds?\b|\byards?\b"
    r"|\bloss\b|\bgain\b|\bclock\b|\bpenalty\b|\bfumbled?\b|\(H:"
)

#: Share of a game's plays stamped 0:00 above which its clock is frozen.
FROZEN_CLOCK_SHARE = 0.2
#: Last fourth-quarter clock (seconds) above which regulation is missing.
TRUNCATED_Q4_SECS = 300

#: indicator -> per-game condition over :func:`game_indicators` columns
INDICATORS: dict[str, pl.Expr] = {
    "unparsed": c("unparsed"),
    "duplicate_rows": c("duplicate_rows") > 0,
    "frozen_clock": c("frozen_clock"),
    "truncated": c("truncated"),
    "down_skips": c("down_skips") >= 2,
    "garbled_names": c("garbled_names") > 0,
}
#: indicators also reported as INFO whenever present
_HARD = ("unparsed", "duplicate_rows", "frozen_clock")
_EXAMPLES = 10


def floor_path(dataset: str) -> Path:
    """Path to the committed per-(indicator, season) ceilings for ``dataset``.

    Args:
        dataset: Registered dataset name.

    Returns:
        The ceilings JSON path; may not exist.

    Example:
        Inspect the shipped ceilings::

            import json
            from tools.validation.checks import pbp_integrity

            json.loads(pbp_integrity.floor_path("cfb_pbp").read_text())["ceilings"]
    """
    return _FLOOR_DIR / f"{dataset}_integrity.json"


def _clock_secs() -> pl.Expr:
    parts = c("clock.displayValue").cast(pl.String).str.extract_groups(r"^(\d{1,2}):(\d{2})$")
    return parts.struct.field("1").cast(pl.Int64) * 60 + parts.struct.field("2").cast(pl.Int64)


def game_indicators(frame: pl.DataFrame) -> pl.DataFrame:
    """One row per game with every integrity indicator.

    Args:
        frame: Play-by-play frame carrying the columns in ``_REQUIRED``.

    Returns:
        ``game_id``, ``season``, ``plays``, ``flagless_share``, ``unparsed``,
        ``duplicate_rows``, ``frozen_clock``, ``truncated``, ``down_skips``
        and ``garbled_names``.

    Example:
        The flagged games of the registered dataset::

            from tools.validation.checks import pbp_integrity
            from tools.validation.registry import resolve

            frame, _ = resolve("cfb_pbp")
            g = pbp_integrity.game_indicators(frame)
            g.filter(g["unparsed"] | g["frozen_clock"])
    """
    flagged = pl.any_horizontal([(c(f) == True).fill_null(False) for f in _FLAGS])  # noqa: E712
    garbled = pl.sum_horizontal(
        # an all-null column (an early season, a role ESPN never fills) arrives as Null dtype
        [c(n).cast(pl.String).str.contains(GARBLED_NAME).fill_null(False).cast(pl.Int64) for n in _NAME_COLUMNS]
    )
    plays = frame.sort("game_id", "game_play_number").with_columns(
        __secs=_clock_secs(),
        __flagged=flagged,
        __garbled=garbled,
        __dup=pl.len().over("game_id", "id") > 1,
    )
    # A jump of two or more downs between consecutive plays of one possession
    # inside one drive means plays are missing (a replayed down stays put, a
    # loss-of-down foul moves one).
    prev = [c(k).shift(1).over("game_id") for k in ("drive.id", "pos_team_id", "start.down")]
    plays = plays.with_columns(
        __skip=(
            (c("drive.id") == prev[0])
            & (c("pos_team_id") == prev[1])
            & (c("start.down") - prev[2] >= 2)
            & (c("start.down") != 1)
        ).fill_null(False)
    )
    q4 = c("period") == 4
    return (
        plays.group_by("game_id")
        .agg(
            season=c("season").first(),
            plays=pl.len(),
            flagless_share=1.0 - c("__flagged").mean(),
            duplicate_rows=(pl.len() - c("id").n_unique()).cast(pl.Int64),
            zero_clock_share=(c("__secs") == 0).sum() / c("__secs").count(),
            max_period=c("period").max(),
            q4_last_secs=c("__secs").filter(q4).min(),
            down_skips=c("__skip").sum().cast(pl.Int64),
            garbled_names=c("__garbled").sum().cast(pl.Int64),
        )
        .with_columns(
            unparsed=c("flagless_share") > 0.5,
            frozen_clock=(c("zero_clock_share") > FROZEN_CLOCK_SHARE).fill_null(False),
            truncated=(c("max_period") < 4)
            | ((c("max_period") == 4) & (c("q4_last_secs") > TRUNCATED_Q4_SECS)).fill_null(False),
        )
        .drop("zero_clock_share", "max_period", "q4_last_secs")
        .sort("season", "game_id")
    )


def season_counts(games: pl.DataFrame) -> pl.DataFrame:
    """Count each indicator per season (games flagged; names for ``garbled_names``).

    Args:
        games: Output of :func:`game_indicators`.

    Returns:
        One row per (indicator, season) with ``count`` and up to ten example ``game_ids``.

    Example:
        Seasons with frozen clocks::

            from tools.validation.checks import pbp_integrity as pi

            counts = pi.season_counts(pi.game_indicators(frame))
            counts.filter(counts["indicator"] == "frozen_clock")
    """
    out = []
    for name, cond in INDICATORS.items():
        size = c("garbled_names").sum() if name == "garbled_names" else pl.len()
        out.append(
            games.filter(cond)
            .group_by("season")
            .agg(count=size.cast(pl.Int64), game_ids=c("game_id").sort().head(_EXAMPLES))
            .with_columns(indicator=pl.lit(name))
        )
    return pl.concat(out).select("indicator", "season", "count", "game_ids").sort("indicator", "season")


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Report integrity counts above their committed ceilings, plus hard failures as INFO.

    Skips (returning an empty list) when the ceilings file is absent or a
    required column is missing.

    Args:
        dataset: Registered dataset name.
        frame: The data frame under validation.
        ctx: Check context; ``pbp_integrity_tolerance_frac`` (default 0.1)
            widens each ceiling by that fraction, at least 2.

    Returns:
        WARN findings for (indicator, season) counts above ceiling + tolerance,
        INFO findings for any unparsed, duplicated or frozen-clock games.

    Example:
        Run over the registered dataset::

            from tools.validation.checks import pbp_integrity
            from tools.validation.registry import resolve

            frame, ctx = resolve("cfb_pbp")
            pbp_integrity.run("cfb_pbp", frame, ctx)
    """
    fp = floor_path(dataset)
    if not fp.exists() or any(col not in frame.columns for col in _REQUIRED):
        return []
    snapshot = json.loads(fp.read_text(encoding="utf-8"))
    ceilings, measured_games = snapshot.get("ceilings", {}), snapshot.get("games", {})
    frac = ctx.thresholds.get("pbp_integrity_tolerance_frac", 0.1)
    games = game_indicators(frame)
    games_now = dict(games.group_by("season").len().iter_rows())
    findings: list[Finding] = []
    for row in season_counts(games).to_dicts():
        examples = ", ".join(str(g) for g in row["game_ids"])
        ceiling = ceilings.get(row["indicator"], {}).get(str(row["season"]))
        then = measured_games.get(str(row["season"]))
        if ceiling is not None and then:
            ceiling = ceiling * games_now.get(row["season"], then) / then
        loc = {"indicator": row["indicator"], "season": row["season"]}
        if ceiling is not None and row["count"] > ceiling + max(2, frac * ceiling):
            findings.append(
                Finding(
                    "pbp_integrity",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{row['indicator']} rose in {row['season']}: {row['count']} vs ceiling {ceiling:.0f} "
                    f"(e.g. games {examples})",
                    locator=loc,
                    expected=round(ceiling, 1),
                    actual=row["count"],
                    metric=round(row["count"] - ceiling, 1),
                    needs_judgment=True,
                )
            )
        elif row["indicator"] in _HARD:
            findings.append(
                Finding(
                    "pbp_integrity",
                    Severity.INFO,
                    ctx.domain,
                    dataset,
                    f"{row['count']} {row['indicator']} game(s) in {row['season']} (e.g. {examples})",
                    locator=loc,
                    actual=row["count"],
                )
            )
    return findings


def _refresh(dataset: str) -> int:
    from tools.validation.registry import resolve

    frame, _ = resolve(dataset)
    games = game_indicators(frame)
    counts = season_counts(games)
    seasons = sorted(frame.get_column("season").drop_nulls().unique().to_list())
    ceilings: dict[str, dict[str, int]] = {name: {str(s): 0 for s in seasons} for name in INDICATORS}
    for row in counts.to_dicts():
        ceilings[row["indicator"]][str(row["season"])] = row["count"]
    _FLOOR_DIR.mkdir(parents=True, exist_ok=True)
    with floor_path(dataset).open("w", encoding="utf-8", newline="\n") as fh:
        n_games = {str(s): n for s, n in games.group_by("season").len().iter_rows()}
        json.dump({"dataset": dataset, "ceilings": ceilings, "games": n_games}, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"wrote {floor_path(dataset)}")
    return 0


def _report(dataset: str, out: str) -> int:
    from tools.validation.registry import resolve

    frame, _ = resolve(dataset)
    games = game_indicators(frame)
    flagged = games.filter(pl.any_horizontal(list(INDICATORS.values())))
    flagged.write_parquet(out)
    print(f"wrote {out}: {flagged.height:,} flagged games of {games.height:,}")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--refresh":
        sys.exit(_refresh(args[1]))
    if len(args) == 3 and args[0] == "--report":
        sys.exit(_report(args[1], args[2]))
    print(__doc__)
    sys.exit(2)
