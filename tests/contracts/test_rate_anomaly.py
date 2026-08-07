import polars as pl
import pytest

from tools.validation.checks import rate_anomaly
from tools.validation.findings import CheckContext, Severity


def _ctx(**kw):
    base = dict(domain="cfb", dataset="cfb_pbp", schema={})
    base.update(kw)
    return CheckContext(**base)


def _season(season: int, games: int, sacks_per_game: int) -> list[dict]:
    """One synthetic season: `games` games each carrying `sacks_per_game` sacks."""
    rows = []
    for g in range(games):
        for s in range(max(sacks_per_game, 1)):
            rows.append({"season": season, "game_id": season * 1000 + g, "sack": s < sacks_per_game})
    return rows


def test_collapsed_season_is_flagged_with_ratio():
    # four healthy seasons at 4 sacks/game, one collapsed season at 0
    rows = []
    for yr in (2010, 2011, 2012, 2014):
        rows += _season(yr, 10, 4)
    rows += _season(2013, 10, 0)
    findings = rate_anomaly.run("cfb_pbp", pl.DataFrame(rows), _ctx())
    assert len(findings) == 1
    f = findings[0]
    assert f.severity is Severity.WARN and f.needs_judgment
    assert f.locator == {"column": "sack", "season": 2013}
    assert f.metric == 0.0
    assert "collapsed in 2013" in f.message


def test_healthy_variation_is_not_flagged():
    """Normal era swing (measured worst non-outage cell was 0.56x) must stay silent."""
    rows = []
    for yr, spg in ((2010, 4), (2011, 4), (2012, 3), (2013, 5), (2014, 4)):
        rows += _season(yr, 10, spg)
    assert rate_anomaly.run("cfb_pbp", pl.DataFrame(rows), _ctx()) == []


def test_too_few_seasons_skips():
    """A median over one or two seasons is not a baseline."""
    rows = _season(2013, 10, 0) + _season(2014, 10, 4)
    assert rate_anomaly.run("cfb_pbp", pl.DataFrame(rows), _ctx()) == []


def test_zero_median_flag_is_skipped_not_divided_by_zero():
    rows = []
    for yr in (2011, 2012, 2013):
        rows += [{"season": yr, "game_id": yr * 100 + g, "sack": False} for g in range(5)]
    assert rate_anomaly.run("cfb_pbp", pl.DataFrame(rows), _ctx()) == []


def test_unregistered_dataset_and_missing_columns_skip():
    frame = pl.DataFrame([{"season": 2013, "game_id": 1, "sack": True}])
    assert rate_anomaly.run("not_registered", frame, _ctx(dataset="not_registered")) == []
    assert rate_anomaly.run("cfb_pbp", pl.DataFrame([{"sack": True}]), _ctx()) == []


@pytest.mark.parametrize("floor,expected", [(0.5, 1), (0.1, 0)])
def test_floor_is_threshold_driven(floor, expected):
    """A partial collapse (0.25x median) fires at the shipped floor but not at a
    lower one -- proving the floor discriminates rather than always firing. A
    rate of exactly 0 is below every positive floor, so it cannot show this."""
    rows = []
    for yr in (2010, 2011, 2012, 2014):
        rows += _season(yr, 10, 4)
    rows += _season(2013, 10, 1)  # 1/game vs median 4/game -> 0.25x
    ctx = _ctx(thresholds={"season_rate_floor": floor})
    assert len(rate_anomaly.run("cfb_pbp", pl.DataFrame(rows), ctx)) == expected
