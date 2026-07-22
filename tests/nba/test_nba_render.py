"""Gates for the action-level text renderer — formulas + counter consistency."""

from __future__ import annotations

import json
import pathlib
import re

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    WNBA_RULES,
    PlayerAttribution,
    build_shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import (
    espn_summary_to_events,
    player_game_logs_from_espn,
)
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
    aux_params_from_espn,
    aux_params_from_pbp,
)
from sportsdataverse.nba.nba_possession_sim.render import (
    officials_from_pbp,
    player_names_from_espn,
    player_names_from_pbp,
    simulate_game_actions,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")

FORMULAS = {
    "Made Shot": re.compile(r"^.+ \d+' .+ \(\d+ PTS\)( \(.+ \d+ AST\))?$"),
    "Missed Shot": re.compile(r"^MISS .+ \d+' .+$"),
    "Free Throw": re.compile(r"^(MISS )?.+ Free Throw \d of \d( \(\d+ PTS\))?$"),
    "Rebound": re.compile(r"^(.+ REBOUND \(Off:\d+ Def:\d+\)|.+ Rebound)$"),
    "Turnover": re.compile(r"^(.+ Turnover \(P\d+\.T\d+\)|.+ Turnover: Shot Clock \(T#\d+\))$"),
    "Foul": re.compile(r"^.+ (P\.FOUL|S\.FOUL) \(P\d+\.T\d+\)( \([A-Z]\.[^)]+\))?$"),
    "Timeout": re.compile(r"^.+ Timeout: Regular \(Full \d+ Short 0\)$"),
    "Substitution": re.compile(r"^SUB: .+ FOR .+$"),
    "Jump Ball": re.compile(r"^Jump Ball .+ vs\. .+: Tip to .+$"),
    "period": re.compile(r"^(Start|End) of \d+(st|nd|rd|th) Period \(\d+:\d\d PM EST\)$"),
    "Steal": re.compile(r"^.+ STEAL \(\d+ STL\)$"),
    "Block": re.compile(r"^.+ BLOCK \(\d+ BLK\)$"),
    "Violation": re.compile(r"^.+ Violation: Delay of game Violation$"),
    "Instant Replay": re.compile(r"^Instant Replay\d+(st|nd|rd|th) Period \(\d+:\d\d PM EST\)$"),
}


@pytest.fixture(scope="module")
def nba_setup():
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    raw = pl.concat(frames, how="diagonal_relaxed")
    shelf = build_shelf(possessions_from_pbp(raw))
    shelf.aux = aux_params_from_pbp(raw)
    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    return shelf, att, player_names_from_pbp(raw), officials_from_pbp(raw)


@pytest.fixture(scope="module")
def rendered(nba_setup):
    shelf, att, names, _ = nba_setup
    return simulate_game_actions(shelf, att, names, np.random.default_rng(7))


def test_every_action_matches_its_type_formula(rendered) -> None:
    _, actions = rendered
    assert len(actions) > 250
    for action in actions:
        pattern = FORMULAS[action["action_type"]]
        assert pattern.match(action["description"]), (
            action["action_type"],
            action["description"],
        )


def test_all_play_types_render(rendered) -> None:
    _, actions = rendered
    types = {a["action_type"] for a in actions}
    assert types <= set(FORMULAS)
    assert {
        "Made Shot",
        "Missed Shot",
        "Free Throw",
        "Rebound",
        "Turnover",
        "Foul",
        "Timeout",
        "Substitution",
        "Jump Ball",
        "period",
        "Steal",
    } <= types
    assert any("AST" in a["description"] for a in actions)  # assisted makes appear
    assert any(a["sub_type"] == "3PT Jump Shot" for a in actions)
    # shooting fouls precede FT trips; period rows bracket every period
    assert any(a["action_type"] == "Foul" and a["sub_type"] == "Shooting" for a in actions)
    assert any(a["action_type"] == "period" and a["sub_type"] == "start" for a in actions)
    assert any(a["action_type"] == "period" and a["sub_type"] == "end" for a in actions)
    assert actions[0]["action_type"] == "period" and actions[1]["action_type"] == "Jump Ball"


def test_running_counters_are_internally_consistent(rendered) -> None:
    final, actions = rendered
    # running scores are monotone and end at the final
    totals = [a["score_home"] + a["score_away"] for a in actions]
    assert totals == sorted(totals)
    assert actions[-1]["score_home"] == final["score_home"]
    assert actions[-1]["score_away"] == final["score_away"]
    # parsing "(N PTS)" back per player reproduces the scoring ledger exactly
    per_player: dict = {}
    parsed_last: dict = {}
    for action in actions:
        match = re.search(r"\((\d+) PTS\)", action["description"])
        if match and not action["description"].startswith("MISS"):
            pid = action["person_id"]
            value = action["shot_value"] if action["action_type"] == "Made Shot" else 1
            per_player[pid] = per_player.get(pid, 0) + value
            parsed_last[pid] = int(match.group(1))
            assert parsed_last[pid] == per_player[pid], action["description"]
    assert sum(per_player.values()) == final["score_home"] + final["score_away"]


def test_deterministic_and_names_resolve(nba_setup) -> None:
    shelf, att, names, _ = nba_setup
    a = simulate_game_actions(shelf, att, names, np.random.default_rng(11))
    b = simulate_game_actions(shelf, att, names, np.random.default_rng(11))
    assert [x["description"] for x in a[1]] == [x["description"] for x in b[1]]
    assert not any(x["player_name"].startswith("#") for x in a[1] if x["person_id"])


def _scoring_path(actions):
    prev, path = (0, 0), []
    for action in actions:
        current = (action["score_home"], action["score_away"])
        if current != prev:
            path.append((action["period"], current))
            prev = current
    return path


def test_same_seed_same_game_across_providers(nba_setup) -> None:
    """The sim-rng/text-rng split: dialect and text options are cosmetic.

    Text draws are template-key-gated, so before the split each provider
    consumed a different number of draws and the same seed simulated a
    DIFFERENT game per dialect. Now the score path is provider-invariant.
    """
    shelf, att, names, officials = nba_setup
    finals, paths = [], []
    for provider, offs in (("nba_stats", officials), ("espn", None), ("ncaa_stats", None)):
        final, actions = simulate_game_actions(
            shelf, att, names, np.random.default_rng(7), provider=provider, officials=offs
        )
        finals.append(final)
        paths.append(_scoring_path(actions))
    assert finals[0] == finals[1] == finals[2]
    assert paths[0] == paths[1] == paths[2]
    # text volume genuinely differs — the invariance is not vacuous
    # (nba_stats stamps officials; ncaa_stats emits assist/sub rows)
    final_no_offs, actions_no_offs = simulate_game_actions(
        shelf, att, names, np.random.default_rng(7), provider="nba_stats", officials=None
    )
    assert final_no_offs == finals[0]
    assert _scoring_path(actions_no_offs) == paths[0]


def test_officials_fit_from_real_fouls_and_stamp_rendered_fouls(nba_setup) -> None:
    shelf, att, names, officials = nba_setup
    # the v3 fixtures stamp real referee names ("J.Tiven") on foul rows
    assert len(officials) >= 3
    assert all(re.match(r"^[A-Z]\.", official) for official in officials)
    _, actions = simulate_game_actions(shelf, att, names, np.random.default_rng(7), officials=officials)
    fouls = [a for a in actions if a["action_type"] == "Foul"]
    assert fouls
    for foul in fouls:
        stamp = re.search(r"\(([A-Z]\.[^)]+)\)$", foul["description"])
        assert stamp and stamp.group(1) in officials, foul["description"]


def test_wnba_renders_with_espn_names() -> None:
    summary = json.loads(pathlib.Path("tests/fixtures/espn/summary_wnba.json").read_text(encoding="utf-8"))
    shelf = build_shelf(espn_summary_to_events(summary))
    shelf.aux = aux_params_from_espn(summary)
    logs = player_game_logs_from_espn(summary)
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    names = player_names_from_espn(summary)
    final, actions = simulate_game_actions(shelf, att, names, np.random.default_rng(3), rules=WNBA_RULES)
    assert len(actions) > 200
    assert not any(a["player_name"].startswith("#") for a in actions if a["person_id"])
    for action in actions:
        assert FORMULAS[action["action_type"]].match(action["description"])


COLLEGE_FORMULAS = {
    "Made Shot": re.compile(r"^.+ made (Layup|Jumper|Three Point Jumper)\.( Assisted by .+\.)?$"),
    "Missed Shot": re.compile(r"^.+ missed (Layup|Jumper|Three Point Jumper)\.$"),
    "Free Throw": re.compile(r"^.+ (made|missed) Free Throw\.$"),
    "Rebound": re.compile(r"^(.+ (Offensive|Defensive) Rebound\.|.+ Deadball Team Rebound\.)$"),
    "Turnover": re.compile(r"^.+ Turnover\.$"),
    "Steal": re.compile(r"^.+ Steal\.$"),
    "Block": re.compile(r"^.+ Block\.$"),
    "Foul": re.compile(r"^Foul on .+\.$"),
    "Timeout": re.compile(r"^( Official TV Timeout|.+  Timeout)$"),
    "Jump Ball": re.compile(r"^Jump Ball won by .+$"),
    "period": re.compile(r"^(End of \d(st|nd|rd|th) (half|Quarter)|End of Game)$"),
}


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_college_dialect_renders_espn_formulas(league: str) -> None:
    from sportsdataverse.nba.nba_possession_sim import MBB_RULES, WBB_RULES

    summary = json.loads(pathlib.Path(f"tests/fixtures/espn/summary_{league}.json").read_text(encoding="utf-8"))
    shelf = build_shelf(espn_summary_to_events(summary))
    shelf.aux = aux_params_from_espn(summary)
    logs = player_game_logs_from_espn(summary)
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    names = player_names_from_espn(summary)
    rules = MBB_RULES if league == "mbb" else WBB_RULES
    final, actions = simulate_game_actions(shelf, att, names, np.random.default_rng(5), rules=rules, provider="espn")
    assert len(actions) > 200
    types = {a["action_type"] for a in actions}
    assert types <= set(COLLEGE_FORMULAS)
    # ESPN college pbp carries no substitution rows and no period-start rows,
    # but it does close each period and the game
    assert "Substitution" not in types
    assert {"Jump Ball", "period"} <= types
    assert actions[-1]["description"] == "End of Game"
    unit = "half" if league == "mbb" else "Quarter"
    assert any(a["description"].endswith(unit) for a in actions if a["action_type"] == "period")
    for action in actions:
        pattern = COLLEGE_FORMULAS[action["action_type"]]
        assert pattern.match(action["description"]), (action["action_type"], action["description"])
    assert any("Assisted by" in a["description"] for a in actions)
    assert any(a["action_type"] == "Steal" for a in actions)
    assert not any(a["player_name"].startswith("#") for a in actions if a["person_id"])


NCAA_FORMULAS = {
    "Made Shot": re.compile(r"^.+ (HOME|AWAY) made (Layup|Two Point Jumper|Three Point Jumper)$"),
    "Missed Shot": re.compile(r"^.+ (HOME|AWAY) missed (Layup|Two Point Jumper|Three Point Jumper)$"),
    "Assist": re.compile(r"^.+ (HOME|AWAY) Assist$"),
    "Free Throw": re.compile(r"^.+ (HOME|AWAY) (made|missed) Free Throw$"),
    "Rebound": re.compile(r"^(.+ (HOME|AWAY) (Offensive|Defensive) Rebound|TEAM Deadball Rebound)$"),
    "Turnover": re.compile(r"^(.+ (HOME|AWAY) Turnover|TEAM Turnover)$"),
    "Steal": re.compile(r"^.+ (HOME|AWAY) Steal$"),
    "Block": re.compile(r"^.+ (HOME|AWAY) Blocked Shot$"),
    "Foul": re.compile(r"^.+ (HOME|AWAY) Commits Foul$"),
    "Timeout": re.compile(r"^(HOME|AWAY) (Team|Media) Timeout$"),
    "Substitution": re.compile(r"^.+ (HOME|AWAY) (Enters|Leaves) Game$"),
}


def test_ncaa_stats_dialect_renders_legacy_grammar() -> None:
    """``provider="ncaa_stats"`` reproduces the stats.ncaa.org legacy verbs.

    The grammar divergences the registry must express: assists are their
    own rows (not a made-shot suffix) and substitutions are Enters/Leaves
    PAIRS — both fitted from the committed stats.ncaa.org pbp fixture.
    """
    from sportsdataverse.nba.nba_possession_sim import MBB_RULES

    summary = json.loads(pathlib.Path("tests/fixtures/espn/summary_mbb.json").read_text(encoding="utf-8"))
    shelf = build_shelf(espn_summary_to_events(summary))
    shelf.aux = aux_params_from_espn(summary)
    logs = player_game_logs_from_espn(summary)
    teams = sorted(int(t) for t in logs["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[1], away_team_id=teams[0])
    names = player_names_from_espn(summary)
    _, actions = simulate_game_actions(
        shelf, att, names, np.random.default_rng(11), rules=MBB_RULES, provider="ncaa_stats"
    )
    types = {a["action_type"] for a in actions}
    assert types <= set(NCAA_FORMULAS)
    assert {"Made Shot", "Assist", "Substitution", "Rebound", "Foul"} <= types
    subs = [a for a in actions if a["action_type"] == "Substitution"]
    enters = [a for a in subs if "Enters Game" in a["description"]]
    leaves = [a for a in subs if "Leaves Game" in a["description"]]
    assert enters and len(enters) == len(leaves)  # always emitted as pairs
    for action in actions:
        pattern = NCAA_FORMULAS[action["action_type"]]
        assert pattern.match(action["description"]), (action["action_type"], action["description"])


def test_violation_and_replay_rows_render(nba_setup, monkeypatch) -> None:
    """Dead-ball officiating rows render the v3 grammar when they fire."""
    import sportsdataverse.nba.nba_possession_sim.render as render_mod

    monkeypatch.setattr(render_mod, "VIOLATION_RATE", 0.2)
    monkeypatch.setattr(render_mod, "REPLAY_RATE", 0.2)
    shelf, att, names, _ = nba_setup
    _, actions = simulate_game_actions(shelf, att, names, np.random.default_rng(3))
    violations = [a for a in actions if a["action_type"] == "Violation"]
    replays = [a for a in actions if a["action_type"] == "Instant Replay"]
    assert violations and replays
    for action in violations + replays:
        assert FORMULAS[action["action_type"]].match(action["description"]), action["description"]
    assert {a["sub_type"] for a in replays} <= set(render_mod._REPLAY_RULINGS)
