"""ESPN-vs-alternate parity harness for processed games (private, experimental).

Runs the same game through the ESPN path and an alternate source, then measures, on the
processed output:

* per-column agreement on the Game on Paper hard columns (:data:`GOP_HARD_COLUMNS`, the
  bracket reads in GOP ``python/app.py:199-322``);
* Pearson correlation + mean absolute difference on EPA / EP / WP;
* row equality per advanced box-score section (:data:`GOP_BOX_SECTIONS`).

Gates are **recorded from observed values and never lowered**: :meth:`ParityReport.gates`
emits the numbers to pin, :meth:`ParityReport.check` fails when a pinned number regresses.
Pairing is on the ESPN play ``id`` by default (Shield / CBS adapters emit ``event_id || playId``);
sources without an id join (Yahoo, Fox) pair on :data:`STATE_KEY`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import polars as pl

_BASE = (
    "clock.displayValue",
    "clock.minutes",
    "clock.seconds",
    "type.id",
    "type.text",
    "type.abbreviation",
    "period",
    "EP_start",
    "EP_end",
    "EPA",
    "wp_before",
    "wp_after",
    "wpa",
    "pos_score_diff_start",
    "pos_score_diff_end",
    "start.pos_team_spread",
)
_SIDE = (
    "down",
    "distance",
    "yardsToEndzone",
    "TimeSecsRem",
    "adj_TimeSecsRem",
    "posTeamTimeouts",
    "defPosTeamTimeouts",
    "ExpScoreDiff",
    "ExpScoreDiff_Time_Ratio",
    "spread_time",
    "pos_team_receives_2H_kickoff",
    "is_home",
    "team.id",
    "pos_team.id",
    "pos_team.name",
    "def_pos_team.id",
    "def_pos_team.name",
    "yardLine",
    "homeScore",
    "awayScore",
    "pos_team_score",
    "def_pos_team_score",
)
#: The 60 flat plays-frame columns GOP's ``_reshape_records`` bracket-reads (D report §2.2).
GOP_HARD_COLUMNS: tuple[str, ...] = _BASE + tuple(f"{p}.{c}" for p in ("start", "end") for c in _SIDE)

#: The eight ``advBoxScore`` sections the page manifest requires (D report §2.1).
GOP_BOX_SECTIONS: tuple[str, ...] = (
    "pass",
    "rush",
    "receiver",
    "team",
    "situational",
    "drives",
    "defensive",
    "turnover",
)

#: Columns that get a correlation + mean-|diff| on top of agreement.
NUMERIC_PARITY_COLUMNS: tuple[str, ...] = ("EPA", "EP_start", "EP_end", "wp_before", "wp_after", "wpa")

#: Composite pairing key for sources whose play ids cannot be mapped to ESPN's.
STATE_KEY: tuple[str, ...] = ("period", "clock.displayValue", "start.down", "start.distance", "start.yardsToEndzone")


@dataclass
class ParityReport:
    """Result of :func:`_compare_plays` / :func:`_compare_processed`.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | n_reference | int | plays in the reference (ESPN) frame |
        | n_candidate | int | plays in the candidate (alternate) frame |
        | n_paired | int | plays matched on the key |
        | missing_columns | list[str] | compared columns absent from the candidate |
        | agreement | dict[str, float] | share of paired rows equal per column (both-null counts as equal; floats within ``tol``) |
        | correlation | dict[str, float] | Pearson r per numeric column over non-null pairs |
        | mean_abs_diff | dict[str, float] | mean absolute difference per numeric column |
        | box_rows | dict[str, dict] | per section: ``n_reference``, ``n_candidate``, ``equal_rows`` |
    """

    n_reference: int
    n_candidate: int
    n_paired: int
    missing_columns: list[str] = field(default_factory=list)
    agreement: dict[str, float] = field(default_factory=dict)
    correlation: dict[str, float] = field(default_factory=dict)
    mean_abs_diff: dict[str, float] = field(default_factory=dict)
    box_rows: dict[str, dict[str, int]] = field(default_factory=dict)

    def gates(self) -> dict[str, Any]:
        """Observed values to pin: agreement + correlation floors, paired share, box row-equality share."""
        return {
            "paired_share": round(self.n_paired / self.n_reference, 4) if self.n_reference else 0.0,
            "agreement": {k: round(v, 4) for k, v in self.agreement.items()},
            "correlation": {k: round(v, 4) for k, v in self.correlation.items()},
            "box_equal_share": {
                k: round(v["equal_rows"] / v["n_reference"], 4) if v["n_reference"] else 1.0
                for k, v in self.box_rows.items()
            },
        }

    def check(self, pinned: dict[str, Any]) -> list[str]:
        """Failures where an observed gate fell below its pinned floor (never the other way)."""
        observed = self.gates()
        fails: list[str] = []
        if observed["paired_share"] < pinned.get("paired_share", 0.0):
            fails.append(f"paired_share {observed['paired_share']} < {pinned['paired_share']}")
        for group in ("agreement", "correlation", "box_equal_share"):
            for k, floor in (pinned.get(group) or {}).items():
                got = observed[group].get(k)
                if got is None or got < floor:
                    fails.append(f"{group}.{k} {got} < {floor}")
        for c in pinned.get("required_columns", []):
            if c in self.missing_columns:
                fails.append(f"column missing: {c}")
        return fails


def _is_numeric(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def _pair(reference: pl.DataFrame, candidate: pl.DataFrame, key: str | Sequence[str]) -> pl.DataFrame:
    """Inner-join reference and candidate on ``key`` (candidate columns suffixed ``_cand``); ids compared as strings."""
    keys = [key] if isinstance(key, str) else list(key)
    ref = reference.with_columns([pl.col(k).cast(pl.Utf8) for k in keys])
    cand = candidate.with_columns([pl.col(k).cast(pl.Utf8) for k in keys])
    # one row per key on each side: late duplicates would inflate the pairing
    ref = ref.unique(subset=keys, keep="first", maintain_order=True)
    cand = cand.unique(subset=keys, keep="first", maintain_order=True)
    return ref.join(cand, on=keys, how="inner", suffix="_cand")


def _compare_plays(
    reference: pl.DataFrame,
    candidate: pl.DataFrame,
    *,
    key: str | Sequence[str] = "id",
    columns: Sequence[str] = GOP_HARD_COLUMNS,
    numeric: Sequence[str] = NUMERIC_PARITY_COLUMNS,
    tol: float = 1e-6,
) -> ParityReport:
    """Per-column agreement of two processed plays frames.

    Args:
        reference: the ESPN-path ``plays_frame``.
        candidate: the alternate-path ``plays_frame`` (same game).
        key: pairing key -- ``"id"`` (ESPN play id) or :data:`STATE_KEY` for state-joined sources.
        columns: columns to score for agreement (default: the GOP hard columns).
        numeric: subset that also gets correlation and mean |diff|.
        tol: absolute tolerance for float equality.

    Returns:
        :class:`ParityReport` (``box_rows`` empty; see :func:`_compare_processed`).
    """
    keys = [key] if isinstance(key, str) else list(key)
    missing = [c for c in columns if c not in candidate.columns]
    compared = [c for c in columns if c in reference.columns and c not in missing and c not in keys]
    paired = _pair(reference, candidate, key)
    report = ParityReport(reference.height, candidate.height, paired.height, missing_columns=missing)
    if paired.is_empty():
        return report

    exprs = []
    for c in compared:
        a, b = pl.col(c), pl.col(f"{c}_cand")
        both_null = a.is_null() & b.is_null()
        if _is_numeric(reference.schema[c]) and _is_numeric(candidate.schema[c]):
            eq = (a.cast(pl.Float64) - b.cast(pl.Float64)).abs() <= tol
        else:
            eq = a.cast(pl.Utf8) == b.cast(pl.Utf8)
        exprs.append((eq | both_null).fill_null(False).mean().alias(c))
    report.agreement = {c: float(v) for c, v in paired.select(exprs).row(0, named=True).items()}

    for c in numeric:
        if c not in compared or not (_is_numeric(reference.schema[c]) and _is_numeric(candidate.schema[c])):
            continue
        a, b = pl.col(c).cast(pl.Float64), pl.col(f"{c}_cand").cast(pl.Float64)
        sub = paired.filter(a.is_not_null() & b.is_not_null())
        if sub.height < 2:
            continue
        stats = sub.select(pl.corr(a, b).alias("r"), (a - b).abs().mean().alias("mad")).row(0, named=True)
        report.correlation[c] = float(stats["r"]) if stats["r"] is not None else float("nan")
        report.mean_abs_diff[c] = float(stats["mad"])
    return report


def _row_signatures(rows: list[dict], columns: Sequence[str], ndigits: int = 4) -> Counter:
    def norm(v: Any) -> str:
        if isinstance(v, float):
            return f"{round(v, ndigits):.{ndigits}f}"
        return str(v)

    return Counter(tuple(norm(r.get(c)) for c in columns) for r in rows)


def _compare_box(
    reference: dict, candidate: dict, sections: Sequence[str] = GOP_BOX_SECTIONS
) -> dict[str, dict[str, int]]:
    """Row equality per ``advBoxScore`` section: rows of the reference with an identical row in the candidate.

    Rows are compared on the columns both sides share, floats rounded to 4 dp, as multisets.
    """
    out: dict[str, dict[str, int]] = {}
    for s in sections:
        ref_rows = list(reference.get(s) or [])
        cand_rows = list(candidate.get(s) or [])
        cols = (
            sorted({k for r in ref_rows for k in r} & {k for r in cand_rows for k in r})
            if ref_rows and cand_rows
            else []
        )
        equal = sum((_row_signatures(ref_rows, cols) & _row_signatures(cand_rows, cols)).values()) if cols else 0
        out[s] = {"n_reference": len(ref_rows), "n_candidate": len(cand_rows), "equal_rows": equal}
    return out


def _compare_processed(reference: Any, candidate: Any, **kwargs: Any) -> ParityReport:
    """Compare two processed games (``ProcessedGame`` or any object with ``plays_frame`` and ``game["advBoxScore"]``).

    ``kwargs`` go to :func:`_compare_plays`.
    """
    report = _compare_plays(reference.plays_frame, candidate.plays_frame, **kwargs)
    report.box_rows = _compare_box(reference.game.get("advBoxScore") or {}, candidate.game.get("advBoxScore") or {})
    return report
