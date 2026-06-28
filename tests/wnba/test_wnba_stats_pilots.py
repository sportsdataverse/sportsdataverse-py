from pathlib import Path

import polars as pl

from sportsdataverse.wnba import wnba_stats
from tests.conftest import skip_if_no_nba_stats_live

FIX = Path(__file__).resolve().parents[1] / "nba" / "fixtures"


def test_pilot_leaguedashplayerstats_wnba_second_host():
    captured = {}
    text = (FIX / "cap_leaguedashplayerstats_wnba.json").read_text(encoding="utf-8")

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["host_header"] = headers.get("Host")
        return 200, text

    df = wnba_stats.wnba_stats_leaguedashplayerstats(transport=transport)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    # the generated wrapper must target stats.wnba.com (second host)
    assert captured["url"] == "https://stats.wnba.com/stats/leaguedashplayerstats"
    assert captured["host_header"] == "stats.wnba.com"


@skip_if_no_nba_stats_live
def test_live_leaguedashplayerstats_wnba():
    df = wnba_stats.wnba_stats_leaguedashplayerstats()
    assert isinstance(df, pl.DataFrame) and df.height > 0
