# dev/run_rapm_validation_report.py  — opt-in, run manually with SDV_PY_NBA_STATS_LIVE=1
from pathlib import Path
from sportsdataverse.nba.nba_season_compile import compile_nba_season
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, validate_model, render_report

seasons = [compile_nba_season(y) for y in (2021, 2022, 2023)]  # multi-hour cached pull
rep = validate_model(RidgeRapmModel(), seasons, model_name="plain_rapm")
Path("dev/nba_rapm_validation_report.md").write_text(render_report(rep), encoding="utf-8")
print(render_report(rep))
