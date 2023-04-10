import sportsdataverse as sdv

def main():
    print(sdv.wbb.espn_wbb_calendar(season=2020))
    print(sdv.wbb.espn_wbb_schedule(dates=2020))
    print(sdv.wnba.espn_wnba_calendar(season=2020))
    print(sdv.wnba.espn_wnba_schedule(dates=2020))
    print(sdv.mbb.espn_mbb_calendar(season=2020))
    print(sdv.mbb.espn_mbb_schedule(dates=2020))
    print(sdv.nba.espn_nba_calendar(season=2020))
    print(sdv.nba.espn_nba_schedule(dates=2020))
    print(sdv.nfl.espn_nfl_calendar(season=2020))
    print(sdv.nfl.espn_nfl_schedule(dates=2020))
    print(sdv.cfb.espn_cfb_calendar(season=2020))
    print(sdv.cfb.espn_cfb_schedule(dates=2020))
    print(sdv.nhl.espn_nhl_calendar(season=2020))
    print(sdv.nhl.espn_nhl_schedule(dates=2020))


if __name__ == "__main__":
    main()
