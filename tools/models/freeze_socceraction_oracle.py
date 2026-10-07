"""Freeze the socceraction oracle for the SPADL + xT parity tests.

Run ONCE in a throwaway virtual environment (never sdv-py's):

    uv venv --python 3.12 /tmp/sa-oracle
    uv pip install --python /tmp/sa-oracle/bin/python "socceraction==1.5.3" "numpy<2" "statsbombpy>=1.13" "multimethod<2"
    /tmp/sa-oracle/bin/python tools/models/freeze_socceraction_oracle.py --game-id 8658 --out tests/fixtures/socceraction

On Windows the interpreter is ``<venv>/Scripts/python.exe``.

Writes ``<game_id>_spadl.csv`` (socceraction's DIRECT StatsBomb converter, with names, after
``play_left_to_right`` so every action attacks left to right), ``<game_id>_xt_fit.json``
(``ExpectedThreat(l=16, w=12)`` fit on those actions alone, saved with ``save_model``) and
``<game_id>_xt_fit_log.txt`` (the iteration count socceraction prints).

socceraction is MIT (c) 2019 KU Leuven Machine Learning Research Group. StatsBomb open data is
free for research and non-commercial use under StatsBomb's public-data license.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", type=int, default=8658)
    ap.add_argument("--out", type=Path, default=Path("tests/fixtures/socceraction"))
    args = ap.parse_args()

    import inspect

    import numpy as np
    import socceraction
    import socceraction.spadl as spadl
    from socceraction.data.statsbomb import StatsBombLoader
    from socceraction.xthreat import ExpectedThreat

    kw = {"getter": "remote"}
    if "creds" in inspect.signature(StatsBombLoader).parameters:
        kw["creds"] = {"user": None, "passwd": None}
    loader = StatsBombLoader(**kw)
    games = None
    for comp in loader.competitions().itertuples():
        g = loader.games(comp.competition_id, comp.season_id)
        if (g.game_id == args.game_id).any():
            games = g[g.game_id == args.game_id]
            break
    if games is None:
        print(f"game {args.game_id} not found in StatsBomb open data", file=sys.stderr)
        return 1
    game = games.iloc[0]
    events = loader.events(args.game_id)
    actions = spadl.statsbomb.convert_to_actions(events, home_team_id=game.home_team_id)
    actions = spadl.play_left_to_right(actions, game.home_team_id)
    actions = spadl.add_names(actions)
    cols = [
        "game_id",
        "original_event_id",
        "action_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "bodypart_id",
        "bodypart_name",
        "type_id",
        "type_name",
        "result_id",
        "result_name",
    ]
    args.out.mkdir(parents=True, exist_ok=True)
    actions[cols].to_csv(args.out / f"{args.game_id}_spadl.csv", index=False, float_format="%.17g", lineterminator="\n")

    model = ExpectedThreat(l=16, w=12)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        model.fit(actions)
    model.save_model(args.out / f"{args.game_id}_xt_fit.json")
    (args.out / f"{args.game_id}_xt_fit_log.txt").write_text(
        f"socceraction {socceraction.__version__}, numpy {np.__version__}, python {sys.version.split()[0]}\n"
        f"game_id {args.game_id}, actions {len(actions)}\n{buf.getvalue()}",
        encoding="utf-8",
    )
    rated = actions[["game_id", "original_event_id", "action_id"]].copy()
    rated["xt"] = model.rate(actions)
    rated.to_csv(args.out / f"{args.game_id}_xt_rate.csv", index=False, float_format="%.17g", lineterminator="\n")
    print(f"wrote {len(actions)} actions and the xT grid to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
