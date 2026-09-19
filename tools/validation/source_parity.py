"""Nightly cross-source parity harness: every registered alternate vs the ESPN path.

H1 of ``ClaudeCowork/plans/2026-09-18-data-integrity-and-live-monitoring.md``. This is the
guard against silent source drift -- a markup change, a new type vocabulary, an id scheme
that shifts -- on the alternate feeds Game on Paper fails over to. It generalises the
per-adapter G1 gate scripts (``s2-shield-adapter/parity_finals.py``, ``s2-cbs-nfl/gate.py``,
``s2-yahoo-cfb/parity.py``, ``s2-ncaa-cfb/g1.py``) rather than re-inventing them: same
``football.sources.parity`` comparison, same one-closing-line-into-both-paths discipline,
same ``|dEPA| > 0.5`` classification.

What it does, for one date (default: yesterday):

1. lists that date's **finals** per league off the ESPN scoreboard (over ``http``: ``https``
   403s from the droplet);
2. for every **registered** alternate -- whatever ``dispatch._ADAPTER_MODULES`` imports today,
   so a source is picked up the day its PR merges, with no edit here -- runs the game through
   ``_process_game(..., fallthrough=False)`` and through the ESPN path;
3. pairs the two processed plays frames per :mod:`sportsdataverse.football.sources.parity`
   (play id where the adapter emits ESPN ids, else game state; the CFB state key drops the
   clock) and writes **one row per (date, league, source, game)**: join rate, play counts,
   EP/WP/EPA correlation, exact-match rates, and the top divergence classes;
4. rolls those up per source and checks them against the accepted floors in
   ``source_parity_floors.yaml``, seeded from the program ledger;
5. writes the per-source summary to the platform telemetry store and raises alert rows for
   a source that drifted.

**Alerts** (there is no Slack/webhook path on the platform -- checked 2026-09-18 -- so an
alert IS a ``platform.error_log`` row, which is what ``GET /v1/admin/errors`` and the
``/platform/admin`` dashboard already read):

* a metric's pooled ``r`` below its floor,
* the day's join rate below ``join_rate`` (default 0.90),
* the source raising ``SourceUnavailable`` on more than ``unavailable_share`` of the games.

**Politeness.** Every HTTP call the package makes goes through ``requests.Session.request``;
this module wraps it with a per-host minimum interval (default 2 s, ``SDV_PARITY_MIN_INTERVAL``)
and turns on sportsdataverse's own filesystem response cache pointed at a dated directory
under ``/mnt/sdv_repos`` (never the root fs, which runs full). So a re-run of a date is
near-free and deterministic, which is also what makes the job **idempotent per date**: the
output parquet is only rebuilt with ``--force``.

Usage (the venv python by absolute path -- systemd's PATH has no ``uv``)::

    /mnt/sdv_repos/sdv-py/.venv/bin/python -m tools.validation.source_parity \
        --date 2026-09-13 --leagues nfl,cfb --out /mnt/sdv_repos/.../h1-parity

Exit code is 0 on a clean run, 2 when any alert fired (so a scheduler can see drift without
parsing the JSON), 1 on a harness error.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import math
import os
import sys
import threading
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl
import yaml

from sportsdataverse.football.sources.dispatch import (
    SOURCE_ORDER,
    _adapter_for,
    _process_game,
)
from sportsdataverse.football.sources.parity import STATE_KEY, _compare_plays, _pair

log = logging.getLogger("source_parity")

#: Metrics correlated and exact-matched on every paired play.
METRICS: tuple[str, ...] = ("EPA", "EP_start", "EP_end", "wp_before", "wp_after", "wpa")

#: Columns scored for agreement on top of the GOP hard columns; also what ``_classify`` reads.
CLASSIFY_COLUMNS: tuple[str, ...] = (
    "type.text",
    "start.down",
    "start.distance",
    "start.yardsToEndzone",
    "end.yardsToEndzone",
)

#: Game-state pairing key for sources with no ESPN play ids: ``parity.STATE_KEY`` with the clock
#: swapped for the possessing club.
#:
#: The clock has to go on CFB -- ESPN's CFB vendor feed repeats a stale clock across consecutive
#: plays (15:00 x4, then 13:52 x3) while the alternates stamp every play, so a clock-bearing key
#: pairs ~8% of the rows (s2-yahoo-cfb/parity.py). It is dropped on NFL too, because this is the
#: key the NFL alternates' own accepted gate measured on: s2-yahoo-nfl pinned 96.9% here and said
#: so explicitly. A first pass kept the clock for NFL (its feed does stamp a distinct clock on
#: nearly every play) and paired 51% of one 2026 game -- the harness generalises the gates, it
#: does not get to join on a different key and then call the gap drift.
STATE_KEYS: dict[str, tuple[str, ...]] = dict.fromkeys(
    ("nfl", "cfb"),
    ("period", "start.down", "start.distance", "start.yardsToEndzone", "start.pos_team.id"),
)
#: Everything ``parity.STATE_KEY`` identifies a play by, except the clock, is still in the key.
assert set(STATE_KEY) - {"clock.displayValue"} <= set(STATE_KEYS["nfl"])

#: Clock-stoppage rows carry the PRECEDING snap's state on both feeds, so on a clock-free key
#: they collide with that snap and pair a Timeout against a Pass Reception. Dropped from both
#: sides of a state join (never from an id join, where they pair correctly).
ADMIN_TYPES: tuple[str, ...] = (
    "Timeout",
    "End Period",
    "End of Half",
    "End of Game",
    "End of Regulation",
    "Official Timeout",
    "Two-minute warning",
)

#: The processor's own fallback line. Injected into BOTH paths when ESPN states no line, so a
#: missing pickcenter cannot move wp on every row of one side only.
DEFAULT_ODDS: dict[str, Any] = {
    "gameSpread": 2.5,
    "overUnder": 55.5,
    "homeFavorite": True,
    "gameSpreadAvailable": False,
}

FLOORS_FILE = Path(__file__).with_name("source_parity_floors.yaml")
#: Response cache + payload root. /mnt/sdv_repos, never the root fs (95% full, see memory).
CACHE_ROOT = Path(os.environ.get("SDV_PARITY_CACHE_ROOT", "/mnt/sdv_repos/.sdv-parity-cache"))
ADMIN_KEY_FILE = Path(os.environ.get("SDV_PLATFORM_ADMIN_KEY_FILE", "/root/.sdv-platform-admin-key"))


# --------------------------------------------------------------------------- politeness


class _HostThrottle:
    """Per-host minimum interval, applied at the one chokepoint every fetch shares.

    Patching ``requests.Session.request`` rather than ``dl_utils.download`` is deliberate:
    the adapters bind ``download`` into their own module namespace at import time, so a patch
    on ``dl_utils`` would miss them, and dl_utils' internal retries would not be throttled at
    all. This sees every request, retries included.
    """

    def __init__(self, min_interval: float) -> None:
        self.min_interval = float(min_interval)
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()
        self._original: Any = None

    def _wait(self, url: str) -> None:
        host = urlsplit(url).netloc.lower()
        with self._lock:
            gap = self.min_interval - (time.monotonic() - self._last.get(host, -1e9))
            if gap > 0:
                time.sleep(gap)
            self._last[host] = time.monotonic()

    def install(self) -> None:
        import requests

        if self._original is not None or self.min_interval <= 0:
            return
        self._original = requests.sessions.Session.request
        original = self._original

        def request(session, method, url, *args, **kwargs):  # type: ignore[no-untyped-def]
            self._wait(str(url))
            return original(session, method, url, *args, **kwargs)

        requests.sessions.Session.request = request  # type: ignore[assignment,method-assign]

    def uninstall(self) -> None:
        import requests

        if self._original is not None:
            requests.sessions.Session.request = self._original  # type: ignore[method-assign]
            self._original = None


def _enable_payload_cache(date: str) -> Path:
    """Point sportsdataverse's filesystem response cache at a dated dir and return it."""
    directory = CACHE_ROOT / date
    directory.mkdir(parents=True, exist_ok=True)
    os.environ["SDV_PY_CACHE_DIR"] = str(directory)
    import sportsdataverse
    from sportsdataverse.cache import set_cache_mode, set_default_ttl

    set_cache_mode("filesystem")
    set_default_ttl(_dt.timedelta(days=30))  # a finished game does not change
    assert sportsdataverse  # import kept for the side effect of a configured package
    return directory


# --------------------------------------------------------------------------- configuration


def load_floors(path: Path | None = None) -> dict[str, Any]:
    """Read ``source_parity_floors.yaml`` (``defaults`` + ``sources``)."""
    raw = yaml.safe_load((path or FLOORS_FILE).read_text()) or {}
    return {"defaults": raw.get("defaults") or {}, "sources": raw.get("sources") or {}}


def source_config(floors: Mapping[str, Any], league: str, source: str) -> dict[str, Any]:
    """Merged config for one source: its entry over ``defaults``. Unknown source => no floors."""
    cfg = dict(floors.get("defaults") or {})
    cfg.update((floors.get("sources") or {}).get(f"{league}.{source}") or {})
    cfg.setdefault("floors", {})
    return cfg


def registered_alternates(league: str) -> tuple[str, ...]:
    """Alternates in ``SOURCE_ORDER[league]`` whose adapter actually imports and registers.

    This is the automatic-pickup hinge: nothing here names an adapter. ``_adapter_for``
    imports the module on first use, so a source appears the day its PR merges and
    disappears (loudly, in the row's ``error``) if its import breaks.
    """
    return tuple(s for s in SOURCE_ORDER.get(league, ()) if s != "espn" and _adapter_for(league, s) is not None)


# --------------------------------------------------------------------------- the day's finals


def finals(league: str, date: str, *, limit: int | None = None) -> list[int]:
    """Completed ESPN event ids for ``date`` (``YYYY-MM-DD``), from the scoreboard over http.

    An empty slate comes back from ESPN as an empty frame with no columns at all (and as
    ``None`` for CFB), which is why the shape is checked before the filter.
    """
    ymd = int(date.replace("-", ""))
    if league == "nfl":
        from sportsdataverse.nfl import espn_nfl_schedule

        frame = espn_nfl_schedule(dates=ymd)
    elif league == "cfb":
        from sportsdataverse.cfb import espn_cfb_schedule

        frame = espn_cfb_schedule(dates=ymd, groups=80, limit=500)
    else:
        raise ValueError(f"league must be nfl or cfb, got {league!r}")
    if frame is None or frame.height == 0 or "status_type_completed" not in frame.columns:
        return []
    ids = frame.filter(pl.col("status_type_completed")).get_column("game_id").cast(pl.Int64).to_list()
    return sorted(ids)[:limit] if limit else sorted(ids)


# --------------------------------------------------------------------------- one game


def _espn_summary(league: str, espn_id: int) -> dict:
    cls, fetch = _processor(league)
    return getattr(cls(gameId=espn_id, raw=True), fetch)()


def _processor(league: str):  # type: ignore[no-untyped-def]
    from sportsdataverse.football.sources.dispatch import _processor_class

    return _processor_class(league)


def espn_odds(summary: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    """ONE closing line for both paths, so a comparison isolates the FEED, not the odds.

    Every adapter gate did this; a half-point difference between ESPN's pickcenter and a
    source's own consensus moves ``wp_before`` on every row of the game and swamps the
    play-state differences the harness exists to see. When ESPN states no line (the norm on
    CFB summaries) the processor's own default goes into both sides rather than letting each
    path pick its own.
    """
    for row in summary.get("pickcenter") or []:
        if row.get("spread") is not None and row.get("overUnder") is not None:
            return {
                "gameSpread": abs(float(row["spread"])),
                "overUnder": float(row["overUnder"]),
                "homeFavorite": bool((row.get("homeTeamOdds") or {}).get("favorite")),
                "gameSpreadAvailable": True,
            }, "espn_pickcenter"
    return dict(DEFAULT_ODDS), "processor_default"


def _snaps(frame: pl.DataFrame, key: Sequence[str]) -> pl.DataFrame:
    """Scrimmage rows whose state key is unique in this game.

    A key that occurs twice on a side cannot identify a play, and ``_pair``'s ``keep="first"``
    would happily pair two different ones. Ambiguous keys are dropped from BOTH sides rather
    than paired wrongly (s2-yahoo-cfb/parity.py).
    """
    keys = [k for k in key if k in frame.columns]
    if not keys:
        return frame
    if "type.text" in frame.columns:
        frame = frame.filter(~pl.col("type.text").is_in(ADMIN_TYPES))
    return frame.filter(pl.struct(keys).count().over(keys) == 1)


def _classify(row: Mapping[str, Any]) -> str:
    """Why one paired row's EPA moved: the two feeds disagree about WHAT, WHERE, or neither."""
    if row.get("EPA") is None or row.get("EPA_cand") is None:
        return "null_metric"
    if row.get("type.text") != row.get("type.text_cand"):
        return "type_label_differs"
    if row.get("start.down") != row.get("start.down_cand") or row.get("start.distance") != row.get(
        "start.distance_cand"
    ):
        return "down_distance_differs"
    if row.get("start.yardsToEndzone") != row.get("start.yardsToEndzone_cand"):
        return "start_spot_differs"
    if row.get("end.yardsToEndzone") != row.get("end.yardsToEndzone_cand"):
        return "end_spot_differs"
    return "same_state_metric_differs"


def compare_game(
    league: str,
    espn_id: int,
    source: str,
    *,
    join: str = "state",
    payloads: Mapping[str, Any] | None = None,
    idmap_row: dict | None = None,
) -> tuple[dict[str, Any], pl.DataFrame | None]:
    """One (league, source, game) row, plus the paired frame for pooling.

    Both paths run the unmodified processor with the SAME injected closing line, and
    ``fallthrough=False`` so a source that cannot serve the game is recorded as unavailable
    instead of quietly being answered by ESPN and scoring a perfect 1.0 against itself.
    """
    row: dict[str, Any] = {
        "league": league,
        "source": source,
        "espn_game_id": str(espn_id),
        "join_key": join,
    }
    payloads = dict(payloads or {})
    try:
        summary = payloads.get("espn") or _espn_summary(league, espn_id)
    except Exception as exc:  # noqa: BLE001 -- an ESPN miss is the reference's problem, not the source's
        row["error"] = f"espn: {type(exc).__name__}: {exc}"
        return row, None
    odds, odds_from = espn_odds(summary)
    row["odds_source"] = odds_from
    try:
        reference = _process_game(
            league, espn_id, source="espn", fallthrough=False, payloads={"espn": summary}, odds_override=odds
        )
    except Exception as exc:  # noqa: BLE001
        row["error"] = f"espn: {type(exc).__name__}: {exc}"
        return row, None
    try:
        candidate = _process_game(
            league,
            espn_id,
            source=source,
            fallthrough=False,
            payloads={k: v for k, v in payloads.items() if k != "espn"},
            odds_override=odds,
            idmap_row=idmap_row,
        )
    except Exception as exc:  # noqa: BLE001 -- SourceUnavailable / AllSourcesFailed / adapter bug
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["unavailable"] = True
        return row, None

    key: Sequence[str] = ["id"] if join == "id" else list(STATE_KEYS[league])
    ref_frame, cand_frame = reference.plays_frame, candidate.plays_frame
    if join != "id":
        ref_frame, cand_frame = _snaps(ref_frame, key), _snaps(cand_frame, key)
    try:
        report = _compare_plays(
            ref_frame,
            cand_frame,
            key=key[0] if len(key) == 1 else key,
            columns=tuple(CLASSIFY_COLUMNS) + METRICS,
            numeric=METRICS,
        )
        paired = _pair(ref_frame, cand_frame, key[0] if len(key) == 1 else key)
    except ValueError as exc:  # a pairing key absent or dtype-split: a real finding, not a crash
        row["error"] = f"pairing: {exc}"
        return row, None

    row.update(
        {
            "served": candidate.provenance["served"],
            "n_espn": report.n_reference,
            "n_source": report.n_candidate,
            "n_paired": report.n_paired,
            "join_rate": round(report.n_paired / report.n_reference, 4) if report.n_reference else 0.0,
            "notes": "; ".join(candidate.provenance.get("notes") or []),
            "contract_ok": bool(candidate.provenance["contract"]["ok"]),
        }
    )
    for metric in METRICS:
        row[f"r_{metric}"] = report.correlation.get(metric)
        row[f"mad_{metric}"] = report.mean_abs_diff.get(metric)
        row[f"exact_{metric}"] = report.agreement.get(metric)
    row["divergence"] = json.dumps(divergence_classes(paired))
    return row, _metric_pairs(paired)


def divergence_classes(paired: pl.DataFrame, threshold: float = 0.5) -> dict[str, int]:
    """Counts of the ``|dEPA| > threshold`` rows by cause, biggest class first."""
    if paired.is_empty() or "EPA" not in paired.columns or "EPA_cand" not in paired.columns:
        return {}
    bad = paired.filter((pl.col("EPA").cast(pl.Float64) - pl.col("EPA_cand").cast(pl.Float64)).abs() > threshold)
    counts = Counter(_classify(r) for r in bad.to_dicts())
    return dict(counts.most_common())


def _metric_pairs(paired: pl.DataFrame) -> pl.DataFrame | None:
    """The ``(reference, candidate)`` metric columns of a paired frame, for pooling across games."""
    cols = [c for c in METRICS if c in paired.columns and f"{c}_cand" in paired.columns]
    if not cols:
        return None
    return paired.select(
        [pl.col(c).cast(pl.Float64) for c in cols] + [pl.col(f"{c}_cand").cast(pl.Float64) for c in cols]
    )


# --------------------------------------------------------------------------- roll-up + alerts


def pooled(frames: Iterable[pl.DataFrame]) -> dict[str, dict[str, float]]:
    """Pooled r / mean-|diff| / exact share per metric over EVERY paired play of the day.

    Pooled, not a mean of per-game r: a per-game Pearson r over a 160-play game is dominated
    by one divergent row, and averaging those makes a bad night look fine (and a 3-play Thursday
    look catastrophic). The gate scripts all pooled; so does this.
    """
    frames = [f for f in frames if f is not None and f.height]
    if not frames:
        return {}
    allrows = pl.concat(frames, how="diagonal_relaxed")
    out: dict[str, dict[str, float]] = {}
    for metric in METRICS:
        if metric not in allrows.columns or f"{metric}_cand" not in allrows.columns:
            continue
        sub = allrows.filter(pl.col(metric).is_not_null() & pl.col(f"{metric}_cand").is_not_null())
        if sub.height < 2:
            continue
        stats = sub.select(
            pl.corr(pl.col(metric), pl.col(f"{metric}_cand")).alias("r"),
            (pl.col(metric) - pl.col(f"{metric}_cand")).abs().mean().alias("mad"),
            ((pl.col(metric) - pl.col(f"{metric}_cand")).abs() <= 1e-6).mean().alias("exact"),
        ).row(0, named=True)
        out[metric] = {
            "r": float(stats["r"]) if stats["r"] is not None else float("nan"),
            "mean_abs_diff": float(stats["mad"]),
            "exact_share": float(stats["exact"]),
            "n": int(sub.height),
        }
    return out


def summarise(
    date: str,
    rows: Sequence[Mapping[str, Any]],
    pools: Mapping[tuple[str, str], list[pl.DataFrame]],
    floors: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Per-source daily summary vs its floor, with the alerts it raised."""
    summaries: list[dict[str, Any]] = []
    for league, source in sorted({(r["league"], r["source"]) for r in rows}):
        mine = [r for r in rows if r["league"] == league and r["source"] == source]
        cfg = source_config(floors, league, source)
        unavailable = sum(1 for r in mine if r.get("unavailable"))
        errored = sum(1 for r in mine if r.get("error") and not r.get("unavailable"))
        ok = [r for r in mine if not r.get("error")]
        n_espn = sum(int(r.get("n_espn") or 0) for r in ok)
        n_paired = sum(int(r.get("n_paired") or 0) for r in ok)
        stats = pooled(pools.get((league, source), []))
        summary: dict[str, Any] = {
            "date": date,
            "league": league,
            "source": source,
            "games": len(mine),
            "games_ok": len(ok),
            "games_unavailable": unavailable,
            "games_errored": errored,
            "unavailable_share": round(unavailable / len(mine), 4) if mine else 0.0,
            "n_espn_plays": n_espn,
            "n_paired_plays": n_paired,
            "join_rate": round(n_paired / n_espn, 4) if n_espn else 0.0,
            "pooled": stats,
            "floors": dict(cfg.get("floors") or {}),
            "accepted": dict(cfg.get("accepted") or {}),
            "alerts": [],
        }
        merged: Counter = Counter()
        for r in ok:
            merged.update(json.loads(r.get("divergence") or "{}"))
        summary["divergence"] = dict(merged.most_common())
        summary["alerts"] = alerts_for(summary, cfg)
        summaries.append(summary)
    return summaries


def alerts_for(summary: Mapping[str, Any], cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The three H1 alert rules. A source with no floors can only trip the availability rules."""
    out: list[dict[str, Any]] = []
    key = f"{summary['league']}.{summary['source']}"
    if summary["games"] and summary["unavailable_share"] > float(cfg.get("unavailable_share", 0.5)):
        out.append(
            {
                "rule": "source_unavailable",
                "source": key,
                "observed": summary["unavailable_share"],
                "threshold": float(cfg.get("unavailable_share", 0.5)),
                "detail": f"{summary['games_unavailable']}/{summary['games']} games unavailable",
            }
        )
    if summary["games_ok"] and summary["join_rate"] < float(cfg.get("join_rate", 0.9)):
        out.append(
            {
                "rule": "join_rate",
                "source": key,
                "observed": summary["join_rate"],
                "threshold": float(cfg.get("join_rate", 0.9)),
                "detail": f"{summary['n_paired_plays']}/{summary['n_espn_plays']} plays paired",
            }
        )
    for metric, floor in (cfg.get("floors") or {}).items():
        observed = (summary["pooled"].get(metric) or {}).get("r")
        if not summary["games_ok"]:
            continue
        # NaN compares False against every floor: pl.corr is NaN on a zero-variance column, so
        # a source whose model never ran and ships a constant must NOT pass silently.
        if observed is None or not math.isfinite(observed) or observed < float(floor):
            out.append(
                {
                    "rule": "correlation_floor",
                    "source": key,
                    "metric": metric,
                    "observed": observed,
                    "threshold": float(floor),
                    "detail": f"pooled r over {(summary['pooled'].get(metric) or {}).get('n', 0)} paired plays",
                }
            )
    return out


# --------------------------------------------------------------------------- telemetry


def _admin_key() -> str | None:
    """The admin-scoped bearer key, from the environment or the root-only key file.

    The file is an env-file (``SDV_DATA_ADMIN_KEY=...`` plus a ``# key_id=... owner=...``
    comment), not a bare secret, so reading it whole sends the comment as the key and the API
    answers "invalid or disabled API key". Parsed, never logged, never written anywhere.
    """
    key = os.environ.get("SDV_DATA_ADMIN_KEY")
    if key:
        return key.strip()
    try:
        text = ADMIN_KEY_FILE.read_text()
    except OSError:
        return None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() in ("SDV_DATA_ADMIN_KEY", "SDV_PLATFORM_ADMIN_KEY"):
            return value.strip().strip("'\"") or None
    return None


def post_telemetry(
    summaries: Sequence[Mapping[str, Any]], *, base_url: str | None = None, timeout: float = 10.0
) -> dict[str, Any]:
    """Write the day's per-source summary (and any alert) to the platform telemetry store.

    ``POST /v1/ingest`` takes ``client_event`` and ``error_log`` rows and needs BOTH the
    bearer API key (admin-scoped, read at runtime from ``/root/.sdv-platform-admin-key`` --
    never copied into a file or a log) and the ``X-SDV-Ingest-Key`` shared secret from
    ``/etc/sdv-db/sdv-db.env``. Fail-open, like every other writer into this store: a
    telemetry outage must not fail the parity run, whose real output is the parquet.

    There is no Slack/webhook notifier on the platform (checked 2026-09-18), so an alert is
    an ``error_log`` row with ``service="source_parity"`` -- exactly what
    ``GET /v1/admin/errors`` and the ``/platform/admin`` dashboard already surface.
    """
    import requests

    base_url = (base_url or os.environ.get("SDV_DATA_API_URL") or "http://127.0.0.1:8000").rstrip("/")
    api_key, ingest_key = _admin_key(), os.environ.get("SDV_INGEST_KEY")
    if not api_key or not ingest_key:
        return {"posted": 0, "skipped": "no admin key or no ingest key in the environment"}
    events: list[dict[str, Any]] = []
    for s in summaries:
        path = f"/parity/{s['date']}/{s['league']}/{s['source']}"
        scalars: dict[str, float] = {
            "join_rate": float(s["join_rate"]),
            "games": float(s["games"]),
            "games_ok": float(s["games_ok"]),
            "unavailable_share": float(s["unavailable_share"]),
        }
        for metric, stats in (s.get("pooled") or {}).items():
            if stats.get("r") is not None and math.isfinite(stats["r"]):
                scalars[f"r_{metric}"] = float(stats["r"])
            if stats.get("exact_share") is not None:
                scalars[f"exact_{metric}"] = float(stats["exact_share"])
        for name, value in scalars.items():
            events.append(
                {
                    "table": "client_event",
                    "row": {
                        "type": "source_parity",
                        "name": f"{s['league']}.{s['source']}.{name}",
                        "value": value,
                        "path": path,
                    },
                }
            )
        for alert in s.get("alerts") or []:
            events.append(
                {
                    "table": "error_log",
                    "row": {
                        "service": "source_parity",
                        "level": "error",
                        "message": (
                            f"{alert['rule']}: {alert['source']} "
                            f"{alert.get('metric', '')} observed={alert['observed']} "
                            f"< floor {alert['threshold']} ({alert['detail']})"
                        ),
                        "path": path,
                        "context": json.dumps({**alert, "date": s["date"]}),
                    },
                }
            )
    posted, errors = 0, []
    for chunk in (events[i : i + 100] for i in range(0, len(events), 100)):
        try:
            resp = requests.post(
                f"{base_url}/v1/ingest",
                json={"events": chunk},
                headers={"Authorization": f"Bearer {api_key}", "X-SDV-Ingest-Key": ingest_key},
                timeout=timeout,
            )
            if resp.status_code >= 300:
                errors.append(f"HTTP {resp.status_code}")
            else:
                posted += int(resp.json().get("accepted", 0))
        except Exception as exc:  # noqa: BLE001 -- fail open
            errors.append(f"{type(exc).__name__}: {exc}")
    return {"posted": posted, "errors": errors}


# --------------------------------------------------------------------------- run


def run(
    date: str,
    leagues: Sequence[str] = ("nfl", "cfb"),
    *,
    out_dir: Path,
    sources: Sequence[str] | None = None,
    max_games: int | None = None,
    floors_path: Path | None = None,
    force: bool = False,
    post: bool = True,
) -> dict[str, Any]:
    """Run the harness for one date and write ``parity_{date}.parquet`` + ``summary_{date}.json``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet, summary_json = out_dir / f"parity_{date}.parquet", out_dir / f"summary_{date}.json"
    if parquet.exists() and summary_json.exists() and not force:
        log.info("%s already done (%s); --force to rebuild", date, parquet)
        return json.loads(summary_json.read_text())

    floors = load_floors(floors_path)
    rows: list[dict[str, Any]] = []
    pools: dict[tuple[str, str], list[pl.DataFrame]] = {}
    for league in leagues:
        alternates = [s for s in registered_alternates(league) if not sources or s in sources]
        games = finals(league, date, limit=max_games)
        log.info("%s %s: %d final(s), alternates: %s", date, league, len(games), ", ".join(alternates) or "none")
        for espn_id in games:
            for source in alternates:
                cfg = source_config(floors, league, source)
                row, pair_frame = compare_game(league, espn_id, source, join=str(cfg.get("join", "state")))
                row["date"] = date
                rows.append(row)
                if pair_frame is not None:
                    pools.setdefault((league, source), []).append(pair_frame)
                log.info(
                    "  %s %s %s: %s",
                    league,
                    espn_id,
                    source,
                    row.get("error") or f"paired {row.get('n_paired')}/{row.get('n_espn')} EPA r {row.get('r_EPA')}",
                )

    summaries = summarise(date, rows, pools, floors)
    frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame({"date": []})
    frame.write_parquet(parquet)
    result = {
        "date": date,
        "leagues": list(leagues),
        "rows": len(rows),
        "sources": summaries,
        "alerts": [a for s in summaries for a in s["alerts"]],
    }
    if post:
        result["telemetry"] = post_telemetry(summaries)
    summary_json.write_text(json.dumps(result, indent=1, default=str))
    return result


def _yesterday() -> str:
    return (_dt.date.today() - _dt.timedelta(days=1)).isoformat()


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default=_yesterday(), help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--leagues", default="nfl,cfb")
    ap.add_argument("--sources", default="", help="restrict to these alternates (comma separated)")
    ap.add_argument("--max-games", type=int, default=None)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--floors", type=Path, default=None)
    ap.add_argument("--force", action="store_true", help="rebuild a date that already has output")
    ap.add_argument("--no-post", action="store_true", help="skip the telemetry write")
    ap.add_argument(
        "--min-interval",
        type=float,
        default=float(os.environ.get("SDV_PARITY_MIN_INTERVAL", "2.0")),
        help="seconds between requests to the same host (env SDV_PARITY_MIN_INTERVAL)",
    )
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)

    _enable_payload_cache(args.date)
    throttle = _HostThrottle(args.min_interval)
    throttle.install()
    try:
        result = run(
            args.date,
            [s.strip() for s in args.leagues.split(",") if s.strip()],
            out_dir=args.out,
            sources=[s.strip() for s in args.sources.split(",") if s.strip()] or None,
            max_games=args.max_games,
            floors_path=args.floors,
            force=args.force,
            post=not args.no_post,
        )
    finally:
        throttle.uninstall()
    for s in result["sources"]:
        log.info(
            "%s %s.%s: %d/%d games, join %.3f, EPA r %s%s",
            s["date"],
            s["league"],
            s["source"],
            s["games_ok"],
            s["games"],
            s["join_rate"],
            (s["pooled"].get("EPA") or {}).get("r"),
            f"  ALERTS: {len(s['alerts'])}" if s["alerts"] else "",
        )
    return 2 if result["alerts"] else 0


if __name__ == "__main__":
    sys.exit(main())
