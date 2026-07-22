"""Golden rendered-pbp fixtures — one seeded sim per (league, provider).

Every sim renderer's output is committed as a fixture under
``tests/fixtures/sim_rendered/{league}_{provider}.json``: the full rendered
play-by-play of ONE seeded simulated game, built end-to-end from the
committed REAL capture fixtures (shelves, name pools, officials, lineups,
and context all fitted from the same payloads the engine tests use). The
files are the renderer contract made concrete — one per league crossed
with its family's pinned canonical providers (see ``PROVIDERS``) — and
``tests/test_sim_rendered_fixtures.py`` regenerates each in-process and
diffs it against the committed file, so any renderer/template change must
ship a deliberate regeneration.

Regenerate all fixtures::

    uv run python -m tools.sim_fixtures.build

Rows are written one per line so drift reviews diff play-by-play.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

SEED = 7
FIXTURES = Path("tests/fixtures")
OUT_DIR = FIXTURES / "sim_rendered"

#: The PINNED golden matrix — one entry per canonical provider per family.
#: Runtime register_provider() calls must never change the committed-fixture
#: contract; shipping a new canonical dialect means extending this pin (the
#: drift gate then demands the regenerated files).
PROVIDERS: Dict[str, Tuple[str, ...]] = {
    "basketball": ("nba_stats", "espn", "ncaa_stats"),
    "football": ("nfl_gsis", "espn"),
    "baseball": ("mlb_statsapi",),
    "hockey": ("nhl_rtss",),
}

#: league -> (family, source fixture paths).
LEAGUES: Dict[str, Tuple[str, List[str]]] = {
    "nba": (
        "basketball",
        [
            "nba_engine/0022100001/playbyplayv3.json",
            "nba_engine/0022200001/playbyplayv3.json",
            "nba_engine/0022300001/playbyplayv3.json",
        ],
    ),
    "nbagl": (
        "basketball",
        [
            "nbagl_engine/2022400003/playbyplayv3.json",
            "nbagl_engine/2022400009/playbyplayv3.json",
        ],
    ),
    "wnba": ("basketball", ["espn/summary_wnba.json"]),
    "mbb": ("basketball", ["espn/summary_mbb.json"]),
    "wbb": ("basketball", ["espn/summary_wbb.json"]),
    "nfl": ("football", ["espn/summary_nfl.json"]),
    "cfb": ("football", ["espn/summary_cfb.json"]),
    "mlb": ("baseball", ["mlb_api/play_by_play_745282.json"]),
    "nhl": ("hockey", ["nhl_api_web/pbp_2024_scf_g7.json"]),
}


def _read(rel: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def _basketball_v3_parts(paths: List[str]) -> Tuple[Any, Any, Dict[int, str], List[str]]:
    from sportsdataverse.nba.nba_possession_sim import (
        PlayerAttribution,
        build_shelf,
        player_game_logs_from_pbp,
        possessions_from_pbp,
    )
    from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_pbp
    from sportsdataverse.nba.nba_possession_sim.render import officials_from_pbp, player_names_from_pbp

    frames = []
    last_gid = ""
    for rel in paths:
        payload = _read(rel)
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        last_gid = rel.split("/")[1]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(last_gid).alias("game_id")))
    raw = pl.concat(frames, how="diagonal_relaxed")
    shelf = build_shelf(possessions_from_pbp(raw))
    shelf.aux = aux_params_from_pbp(raw)
    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == last_gid)
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    return shelf, att, player_names_from_pbp(raw), officials_from_pbp(raw)


def _basketball_espn_parts(path: str) -> Tuple[Any, Any, Dict[int, str], List[str]]:
    from sportsdataverse.nba.nba_possession_sim import PlayerAttribution, build_shelf
    from sportsdataverse.nba.nba_possession_sim.espn_adapter import (
        espn_summary_to_events,
        player_game_logs_from_espn,
    )
    from sportsdataverse.nba.nba_possession_sim.expanded_nodes import aux_params_from_espn
    from sportsdataverse.nba.nba_possession_sim.render import player_names_from_espn

    summary = _read(path)
    shelf = build_shelf(espn_summary_to_events(summary))
    shelf.aux = aux_params_from_espn(summary)
    logs = player_game_logs_from_espn(summary)
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    return shelf, att, player_names_from_espn(summary), []


def _build_basketball(league: str, provider: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    from sportsdataverse.nba.nba_possession_sim import MBB_RULES, NBA_RULES, WBB_RULES, WNBA_RULES
    from sportsdataverse.nba.nba_possession_sim.render import simulate_game_actions

    _family, paths = LEAGUES[league]
    if league in ("nba", "nbagl"):
        shelf, att, names, officials = _basketball_v3_parts(paths)
    else:
        shelf, att, names, officials = _basketball_espn_parts(paths[0])
    rules = {"nba": NBA_RULES, "nbagl": NBA_RULES, "wnba": WNBA_RULES, "mbb": MBB_RULES, "wbb": WBB_RULES}[league]
    final, rows = simulate_game_actions(
        shelf,
        att,
        names,
        np.random.default_rng(SEED),
        rules=rules,
        provider=provider,
        officials=officials or None,
    )
    return dict(final), rows


def _build_football(league: str, provider: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    from sportsdataverse.nfl.nfl_drive_sim import (
        build_football_shelf,
        football_names_from_espn,
        plays_from_espn_drives,
        render_football_pbp,
        simulate_football_game_pbp,
    )

    summary = _read(LEAGUES[league][1][0])
    shelf = build_football_shelf(plays_from_espn_drives(summary))
    names = football_names_from_espn(summary)
    rng = np.random.default_rng(SEED)
    st, pbp = simulate_football_game_pbp(shelf, rng, college_ot=(league == "cfb"))
    rows = render_football_pbp(pbp, names, rng, provider=provider)
    return {"score_home": st.score_home, "score_away": st.score_away}, rows


def _build_baseball(league: str, provider: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    from sportsdataverse.mlb.mlb_at_bat_sim import (
        at_bats_from_pbp,
        build_at_bat_pmf,
        lineups_from_pbp,
        render_mlb_game_pbp,
    )

    payload = _read(LEAGUES[league][1][0])
    pmf = build_at_bat_pmf(at_bats_from_pbp(payload))
    lineups = lineups_from_pbp(payload)
    (away, home), rows = render_mlb_game_pbp(pmf, lineups, np.random.default_rng(SEED), provider=provider)
    return {"score_away": away, "score_home": home}, rows


def _build_hockey(league: str, provider: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    from sportsdataverse.nhl.nhl_game_sim import (
        build_nhl_shelf,
        events_from_nhl_pbp,
        nhl_event_shares_from_pbp,
        nhl_names_from_pbp,
        nhl_render_context_from_pbp,
        render_nhl_game_pbp,
    )

    payload = _read(LEAGUES[league][1][0])
    shelf = build_nhl_shelf(events_from_nhl_pbp(payload))
    final, rows = render_nhl_game_pbp(
        shelf,
        nhl_event_shares_from_pbp(payload),
        nhl_names_from_pbp(payload),
        np.random.default_rng(SEED),
        nhl_render_context_from_pbp(payload),
        provider=provider,
    )
    return {"score_home": final.score_home, "score_away": final.score_away}, rows


_BUILDERS = {
    "basketball": _build_basketball,
    "football": _build_football,
    "baseball": _build_baseball,
    "hockey": _build_hockey,
}


def manifest() -> List[Tuple[str, str]]:
    """Every (league, provider) pair — leagues crossed with the pinned
    canonical providers of their sport family."""
    pairs: List[Tuple[str, str]] = []
    for league, (family, _paths) in LEAGUES.items():
        for provider in sorted(PROVIDERS[family]):
            pairs.append((league, provider))
    return pairs


def build_fixture(league: str, provider: str) -> Dict[str, Any]:
    """Build one league's rendered game under one provider's templates.

    Args:
        league: One of the :data:`LEAGUES` keys.
        provider: A provider registered for the league's sport family.

    Returns:
        The fixture object: ``meta`` (provenance + final score, no
        timestamps — the build is fully deterministic) and ``rows``.
    """
    family, paths = LEAGUES[league]
    final, rows = _BUILDERS[family](league, provider)
    return {
        "meta": {
            "league": league,
            "family": family,
            "provider": provider,
            "seed": SEED,
            "source_fixtures": [f"tests/fixtures/{p}" for p in paths],
            "generator": "tools/sim_fixtures/build.py",
            "regenerate": "uv run python -m tools.sim_fixtures.build",
            "final": final,
            "row_count": len(rows),
        },
        "rows": rows,
    }


def render_fixture_text(fixture: Dict[str, Any]) -> str:
    """Serialize with one row per line (reviewable play-by-play diffs)."""
    meta = json.dumps(fixture["meta"], indent=1, sort_keys=True)
    rows = ",\n".join(json.dumps(row, separators=(", ", ": ")) for row in fixture["rows"])
    return '{\n"meta": ' + meta + ',\n"rows": [\n' + rows + "\n]\n}\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for league, provider in manifest():
        fixture = build_fixture(league, provider)
        path = OUT_DIR / f"{league}_{provider}.json"
        path.write_text(render_fixture_text(fixture), encoding="utf-8", newline="\n")
        meta = fixture["meta"]
        print(f"{path}  rows={meta['row_count']}  final={meta['final']}")


if __name__ == "__main__":
    main()
