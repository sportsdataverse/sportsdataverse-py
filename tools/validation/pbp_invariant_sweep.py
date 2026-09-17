"""Offline processor-invariant sweep over the committed ESPN summary libraries.

Samples completed games per season from the local raw libraries, runs
``NFLPlayProcess`` / ``CFBPlayProcess`` on each stored summary with the
network disabled (``summary=`` injection, ``join_participants=False``, every
``download`` patched to raise), evaluates
:mod:`tools.validation.checks.pbp_invariants` on the processed plays, and
aggregates violation rates by league, era and season.

Layout under ``--out``::

    manifest.json                      the sampled games (reused on re-runs)
    games/{league}/{season}/{id}.json  per-game rule results (resume marker)
    plays/{league}/{season}/{id}.parquet  slim processed plays for triage
    rules_by_era.csv / rules_by_season.csv / examples.json / td_end_ep.csv

Usage (resumable; finished games are skipped)::

    PYTHONPATH=<sdv-py checkout> python -m tools.validation.pbp_invariant_sweep all \\
        --out <dir> --nfl-raw /mnt/sdv_repos/nfl-raw/nfl/espn \\
        --cfb-raw /mnt/sdv_repos/cfbfastR-cfb-raw --workers 3

``sample`` / ``run`` / ``report`` run the stages separately; ``rescore``
re-evaluates changed rules on the saved plays frames without re-processing.
Use a fresh ``--out`` (keep ``manifest.json``) to re-measure after processor
fixes -- the plays must be re-processed for a processor change to show.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import json
import logging
import multiprocessing as mp
import os
import random
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

log = logging.getLogger("pbp_invariant_sweep")

#: ESPN conference ids that are FBS (current + historical: Big East 10, WAC 16).
FBS_CONFERENCES = {"1", "4", "5", "8", "9", "10", "12", "15", "16", "17", "18", "37", "151"}

ERAS = ((2002, 2009), (2010, 2014), (2015, 2019), (2020, 2024), (2025, 2026))


def era_of(season: int) -> str:
    for lo, hi in ERAS:
        if lo <= season <= hi:
            return f"{lo}-{hi}"
    return str(season)


# ---------------------------------------------------------------------------
# raw library access
# ---------------------------------------------------------------------------


def nfl_summary_path(root: Path, season: int, game_id: int) -> Path:
    return root / "raw" / str(season) / f"{game_id}.json.gz"


def cfb_summary_path(root: Path, game_id: int) -> Path:
    return root / "cfb" / "json" / "raw" / f"{game_id}.json"


def read_summary(league: str, root: Path, season: int, game_id: int) -> dict[str, Any] | None:
    path = nfl_summary_path(root, season, game_id) if league == "nfl" else cfb_summary_path(root, game_id)
    if not path.exists():
        return None
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                return json.load(fh)
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def summary_facts(summary: dict[str, Any]) -> dict[str, Any]:
    """Completion, OT, text presence, play count and home group of a stored summary."""
    from tools.validation.checks.pbp_invariants import raw_plays

    try:
        comp = summary["header"]["competitions"][0]
    except (KeyError, IndexError, TypeError):
        return {"completed": False}
    status = (comp.get("status") or {}).get("type") or {}
    plays = raw_plays(summary)
    home = next((x for x in comp.get("competitors") or [] if x.get("homeAway") == "home"), {})
    groups = (home.get("team") or {}).get("groups")
    return {
        "completed": status.get("state") == "post" or bool(status.get("completed")),
        "ot": "OT" in str(status.get("detail") or status.get("shortDetail") or ""),
        "n_plays": len(plays),
        "has_text": any(p.get("text") for p in plays),
        "season": (summary.get("header") or {}).get("season", {}).get("year"),
        "season_type": (summary.get("header") or {}).get("season", {}).get("type"),
        "home_group": str(groups.get("id")) if isinstance(groups, dict) else None,
    }


# ---------------------------------------------------------------------------
# sampling
# ---------------------------------------------------------------------------


def _take(pool: list[dict[str, Any]], n: int, used: set[int]) -> list[dict[str, Any]]:
    out = []
    for g in pool:
        if len(out) >= n:
            break
        if g["game_id"] not in used:
            used.add(g["game_id"])
            out.append(g)
    return out


def sample_nfl(root: Path, per_season: int, seed: int) -> list[dict[str, Any]]:
    """Per season: up to 3 POST, up to 2 OT, the rest REG; only completed games with play text."""
    games = json.loads((root / "crosswalk" / "games.json").read_text())
    rng = random.Random(seed)
    by_season: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for g in games:
        if g.get("status") == "STATUS_FINAL" and int(g.get("season_type") or 0) in (2, 3):
            by_season[int(g["season"])].append(g)
    out: list[dict[str, Any]] = []
    for season in sorted(by_season):
        cands = by_season[season][:]
        rng.shuffle(cands)
        post, reg, ot = [], [], []
        # scan until quotas fill (OT needs the header, so read summaries)
        for n_read, g in enumerate(cands):
            if (len(post) >= 3 and len(reg) >= per_season and len(ot) >= 2) or n_read >= 90:
                break
            gid = int(g["espn_event_id"])
            summary = read_summary("nfl", root, season, gid)
            if not summary:
                continue
            facts = summary_facts(summary)
            if not (facts["completed"] and facts["has_text"] and facts["n_plays"] >= 50):
                continue
            row = {"league": "nfl", "season": season, "game_id": gid}
            if facts["ot"] and len(ot) < 2:
                ot.append({**row, "stratum": "OT"})
            elif int(g["season_type"]) == 3 and len(post) < 3:
                post.append({**row, "stratum": "POST"})
            elif int(g["season_type"]) == 2 and len(reg) < per_season:
                reg.append({**row, "stratum": "REG"})
        used: set[int] = set()
        picked = _take(ot, 2, used) + _take(post, 3, used)
        picked += _take(reg, max(0, per_season - len(picked)), used)
        out += picked
        log.info("nfl %s: sampled %d (ot=%d post=%d)", season, len(picked), len(ot), len(post))
    return out


def sample_cfb(root: Path, per_season: int, seed: int) -> list[dict[str, Any]]:
    """Per season: 2 OT, 2 POST, 2 lower-division home games, the rest FBS regular season."""
    import polars as pl

    rng = random.Random(seed)
    master = pl.read_parquet(root / "cfb" / "cfb_schedule_master.parquet").filter(
        pl.col("status_type_completed") == True  # noqa: E712
    )
    raw_dir = root / "cfb" / "json" / "raw"
    on_disk = {int(p.name.split(".")[0]) for p in raw_dir.glob("*.json")}
    rows = [r for r in master.iter_rows(named=True) if int(r["game_id"]) in on_disk]
    known = {int(r["game_id"]) for r in rows}
    by_season: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        season, gid = int(r["season"]), int(r["game_id"])
        detail = str(r.get("status_type_detail") or "")
        item = {"league": "cfb", "season": season, "game_id": gid}
        if "OT" in detail:
            by_season[season]["OT"].append({**item, "stratum": "OT"})
        elif int(r.get("season_type") or 2) != 2:
            by_season[season]["POST"].append({**item, "stratum": "POST"})
        elif str(r.get("home_conference_id")) not in FBS_CONFERENCES:
            by_season[season]["FCS_HOME"].append({**item, "stratum": "FCS_HOME"})
        else:
            by_season[season]["REG"].append({**item, "stratum": "REG"})
    # games absent from the FBS master are lower-division matchups: season from the header
    extras = sorted(on_disk - known)
    rng.shuffle(extras)
    for gid in extras[:600]:
        summary = read_summary("cfb", root, 0, gid)
        facts = summary_facts(summary or {})
        if facts.get("completed") and facts.get("season") and facts.get("n_plays", 0) >= 50 and facts.get("has_text"):
            s = int(facts["season"])
            by_season[s]["FCS_HOME"].append({"league": "cfb", "season": s, "game_id": gid, "stratum": "FCS_HOME"})
    out: list[dict[str, Any]] = []
    for season in sorted(by_season):
        strata = by_season[season]
        for pool in strata.values():
            rng.shuffle(pool)
        picked: list[dict[str, Any]] = []
        used: set[int] = set()
        for name, n in (("OT", 2), ("POST", 2), ("FCS_HOME", 2)):
            for g in strata[name]:
                if sum(1 for p in picked if p["stratum"] == name) >= n:
                    break
                summary = read_summary("cfb", root, season, g["game_id"])
                facts = summary_facts(summary or {})
                if facts.get("completed") and facts.get("has_text") and facts.get("n_plays", 0) >= 50:
                    picked += _take([g], 1, used)
        for g in strata["REG"]:
            if len(picked) >= per_season:
                break
            summary = read_summary("cfb", root, season, g["game_id"])
            facts = summary_facts(summary or {})
            if facts.get("completed") and facts.get("has_text") and facts.get("n_plays", 0) >= 50:
                picked += _take([g], 1, used)
        out += picked
        log.info("cfb %s: sampled %d", season, len(picked))
    return out


# ---------------------------------------------------------------------------
# processing (spawned workers)
# ---------------------------------------------------------------------------


def _no_network(*_a: Any, **_k: Any) -> Any:
    raise RuntimeError("network call attempted during an offline invariant sweep")


def process_game(task: dict[str, Any]) -> dict[str, Any]:
    """Worker: process one stored game offline, evaluate the invariants, persist results."""
    import polars as pl

    logging.disable(logging.WARNING)
    import sportsdataverse.dl_utils as dl_utils

    dl_utils.download = _no_network
    from tools.validation.checks import pbp_invariants

    league, season, gid = task["league"], int(task["season"]), int(task["game_id"])
    out_dir, raw_root = Path(task["out"]), Path(task["raw_root"])
    game_path = out_dir / "games" / league / str(season) / f"{gid}.json"
    meta: dict[str, Any] = {**{k: task[k] for k in ("league", "season", "game_id", "stratum")}}
    t0 = time.time()
    try:
        summary = read_summary(league, raw_root, season, gid)
        if summary is None:
            raise FileNotFoundError(f"no stored summary for {league} {gid}")
        if league == "nfl":
            import sportsdataverse.nfl.nfl_pbp as mod

            mod.download = _no_network
            proc = mod.NFLPlayProcess(gameId=gid, join_participants=False)
            proc.espn_nfl_pbp(summary=copy.deepcopy(summary))
        else:
            import sportsdataverse.cfb.cfb_pbp as mod

            mod.download = _no_network
            proc = mod.CFBPlayProcess(gameId=gid, join_participants=False)
            proc.espn_cfb_pbp(summary=copy.deepcopy(summary))
        result = proc.run_processing_pipeline() or {}
        plays = getattr(proc, "plays_frame", None)
        if not isinstance(plays, pl.DataFrame) or plays.height == 0:
            meta.update(status="no_plays", seconds=round(time.time() - t0, 1), rules=[])
        else:
            box_team = (result.get("advBoxScore") or {}).get("team")
            plays_path = out_dir / "plays" / league / str(season) / f"{gid}.parquet"
            plays_path.parent.mkdir(parents=True, exist_ok=True)
            # the whole frame, so later rules can be re-scored without re-processing
            plays.write_parquet(plays_path)
            rules = pbp_invariants.evaluate(plays, summary=summary, box={"team": box_team}, league=league)
            meta.update(
                status="ok",
                seconds=round(time.time() - t0, 1),
                n_plays=plays.height,
                box_team=box_team,
                rules=[r.to_dict() for r in rules],
            )
    except Exception as exc:  # noqa: BLE001 -- a processor crash is itself a finding
        meta.update(
            status="error",
            seconds=round(time.time() - t0, 1),
            error=f"{type(exc).__name__}: {exc}"[:500],
            traceback=traceback.format_exc()[-2000:],
            rules=[],
        )
    game_path.parent.mkdir(parents=True, exist_ok=True)
    game_path.write_text(json.dumps(meta, default=str))
    return {k: meta.get(k) for k in ("league", "season", "game_id", "status", "seconds", "error")}


def rescore_game(args: tuple[str, str, str]) -> bool:
    """Worker: re-evaluate one saved game in place; False when it has no saved frame."""
    import polars as pl

    from tools.validation.checks import pbp_invariants

    game_file, out_dir, raw_root = args
    game_path, out = Path(game_file), Path(out_dir)
    meta = json.loads(game_path.read_text())
    league, season, gid = meta["league"], int(meta["season"]), int(meta["game_id"])
    plays_path = out / "plays" / league / str(season) / f"{gid}.parquet"
    if meta.get("status") != "ok" or not plays_path.exists():
        return False
    summary = read_summary(league, Path(raw_root), season, gid)
    box = {"team": meta["box_team"]} if meta.get("box_team") else None
    rules = pbp_invariants.evaluate(pl.read_parquet(plays_path), summary=summary, box=box, league=league)
    meta["rules"] = [r.to_dict() for r in rules]
    game_path.write_text(json.dumps(meta, default=str))
    return True


def rescore(out: Path, roots: dict[str, Path], workers: int = 3) -> None:
    """Re-evaluate the current rules on every saved plays frame (no re-processing)."""
    tasks = [
        (str(p), str(out), str(roots[p.parts[-3]]))
        for p in sorted((out / "games").rglob("*.json"))
        if p.parts[-3] in roots
    ]
    with ProcessPoolExecutor(max_workers=workers, mp_context=mp.get_context("spawn")) as pool:
        n = sum(pool.map(rescore_game, tasks, chunksize=8))
    log.info("re-scored %d of %d games", n, len(tasks))


def _interleave(games: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Round-robin across (league, season) so a partial run still covers every season."""
    buckets: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for g in games:
        buckets[(g["league"], int(g["season"]))].append(g)
    queues = [buckets[k] for k in sorted(buckets)]
    out = []
    while any(queues):
        for q in queues:
            if q:
                out.append(q.pop(0))
    return out


def run(manifest: list[dict[str, Any]], out: Path, roots: dict[str, Path], workers: int) -> None:
    todo = [
        {**g, "out": str(out), "raw_root": str(roots[g["league"]])}
        for g in _interleave(manifest)
        if not (out / "games" / g["league"] / str(g["season"]) / f"{g['game_id']}.json").exists()
    ]
    log.info("%d of %d games to process with %d workers", len(todo), len(manifest), workers)
    for var in ("POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ.setdefault(var, "2")
    done = 0
    with ProcessPoolExecutor(max_workers=workers, mp_context=mp.get_context("spawn")) as pool:
        futures = [pool.submit(process_game, t) for t in todo]
        for fut in as_completed(futures):
            done += 1
            r = fut.result()
            log.info(
                "[%d/%d] %s %s %s %s %ss %s",
                done,
                len(todo),
                r["league"],
                r["season"],
                r["game_id"],
                r["status"],
                r["seconds"],
                r.get("error") or "",
            )


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def report(out: Path) -> None:
    import polars as pl

    metas = [json.loads(p.read_text()) for p in sorted((out / "games").rglob("*.json"))]
    status = pl.DataFrame(
        [{k: m.get(k) for k in ("league", "season", "game_id", "stratum", "status", "seconds", "error")} for m in metas]
    )
    status.write_csv(out / "game_status.csv")
    long_rows = []
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in metas:
        for r in m.get("rules") or []:
            long_rows.append(
                {
                    "league": m["league"],
                    "season": int(m["season"]),
                    "era": era_of(int(m["season"])),
                    "stratum": m["stratum"],
                    "game_id": int(m["game_id"]),
                    "rule": r["rule"],
                    "invariant": r["invariant"],
                    "severity": r["severity"],
                    "n_checked": r["n_checked"],
                    "n_violations": r["n_violations"],
                }
            )
            key = f"{m['league']}|{r['rule']}"
            if r["n_violations"] and len(examples[key]) < 8:
                examples[key].append({"game_id": m["game_id"], "season": m["season"], "samples": r["samples"][:3]})
    if not long_rows:
        log.warning("no rule results under %s", out)
        return
    df = pl.DataFrame(long_rows)
    df.write_parquet(out / "rules_long.parquet")

    def agg(keys: list[str]) -> pl.DataFrame:
        return (
            df.group_by(keys)
            .agg(
                games=pl.col("game_id").n_unique(),
                games_violating=pl.col("game_id").filter(pl.col("n_violations") > 0).n_unique(),
                n_checked=pl.col("n_checked").sum(),
                n_violations=pl.col("n_violations").sum(),
                severity=pl.col("severity").first(),
                invariant=pl.col("invariant").first(),
            )
            .with_columns(rate=pl.col("n_violations") / pl.col("n_checked").clip(lower_bound=1))
            .sort(keys)
        )

    agg(["league", "rule", "era"]).write_csv(out / "rules_by_era.csv")
    agg(["league", "rule", "season"]).write_csv(out / "rules_by_season.csv")
    agg(["league", "rule"]).write_csv(out / "rules_overall.csv")
    (out / "examples.json").write_text(json.dumps(examples, indent=1, default=str))

    plays = []
    for p in sorted((out / "plays").rglob("*.parquet")):
        league, season = p.parts[-3], int(p.parts[-2])
        f = pl.read_parquet(p)
        if not {"rush_td", "pass_td", "EP_end"} <= set(f.columns):
            continue
        plays.append(
            f.filter((pl.col("rush_td") == True) | (pl.col("pass_td") == True)).select(  # noqa: E712
                pl.lit(league).alias("league"), pl.lit(season).alias("season"), pl.col("EP_end").cast(pl.Float64)
            )
        )
    if plays:
        (
            pl.concat(plays)
            .with_columns(era=pl.col("season").map_elements(era_of, return_dtype=pl.Utf8), ep=pl.col("EP_end").round(2))
            .group_by(["league", "era", "ep"])
            .len()
            .sort(["league", "era", "len"], descending=[False, False, True])
            .write_csv(out / "td_end_ep.csv")
        )
    log.info("report written under %s (%d games)", out, len(metas))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=["sample", "run", "rescore", "report", "all"])
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--nfl-raw", type=Path, default=os.environ.get("SDV_VALIDATION_NFL_RAW_ROOT"))
    ap.add_argument("--cfb-raw", type=Path, default=os.environ.get("SDV_VALIDATION_CFB_RAW_ROOT"))
    ap.add_argument("--leagues", default="nfl,cfb")
    ap.add_argument("--nfl-per-season", type=int, default=20)
    ap.add_argument("--cfb-per-season", type=int, default=15)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--seed", type=int, default=20260917)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    leagues = [lg.strip() for lg in args.leagues.split(",") if lg.strip()]
    roots = {"nfl": args.nfl_raw, "cfb": args.cfb_raw}
    for lg in leagues if args.stage != "report" else ():
        if roots.get(lg) is None:
            ap.error(f"--{lg}-raw (or SDV_VALIDATION_{lg.upper()}_RAW_ROOT) is required")
    args.out.mkdir(parents=True, exist_ok=True)
    manifest_path = args.out / "manifest.json"
    if args.stage in ("sample", "all") and not manifest_path.exists():
        manifest: list[dict[str, Any]] = []
        if "nfl" in leagues:
            manifest += sample_nfl(Path(roots["nfl"]), args.nfl_per_season, args.seed)
        if "cfb" in leagues:
            manifest += sample_cfb(Path(roots["cfb"]), args.cfb_per_season, args.seed)
        manifest_path.write_text(json.dumps(manifest, indent=1))
        log.info("manifest: %d games", len(manifest))
    if args.stage in ("run", "all"):
        manifest = [g for g in json.loads(manifest_path.read_text()) if g["league"] in leagues]
        run(manifest, args.out, {k: Path(v) for k, v in roots.items() if v is not None}, min(args.workers, 3))
    if args.stage == "rescore":
        rescore(
            args.out, {k: Path(v) for k, v in roots.items() if v is not None and k in leagues}, min(args.workers, 3)
        )
    if args.stage in ("report", "all"):
        report(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
