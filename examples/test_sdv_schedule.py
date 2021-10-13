import sportsdataverse as sdv

def main():
    print(sdv.wbb.wbb_calendar(season=2020))
    print(sdv.wbb.wbb_schedule(dates=2020))
    print(sdv.wnba.wnba_calendar(season=2020))
    print(sdv.wnba.wnba_schedule(dates=2020))
    print(sdv.mbb.mbb_calendar(season=2020))
    print(sdv.mbb.mbb_schedule(dates=2020))
    print(sdv.nba.nba_calendar(season=2020))
    print(sdv.nba.nba_schedule(dates=2020))
    print(sdv.nfl.nfl_calendar(season=2020))
    print(sdv.nfl.nfl_schedule(dates=2020))
    print(sdv.cfb.cfb_calendar(season=2020))
    print(sdv.cfb.cfb_schedule(dates=2020))
    print(sdv.nhl.nhl_calendar(season=2020))
    print(sdv.nhl.nhl_schedule(dates=2020))


if __name__ == "__main__":
    main()
