import sportsdataverse as sdv

def main():
    print(sdv.wbb.espn_wbb_pbp(game_id=401266534))
    print(sdv.wnba.espn_wnba_pbp(game_id=401370395))
    print(sdv.mbb.espn_mbb_pbp(game_id=401265031))
    print(sdv.nba.espn_nba_pbp(game_id=401307514))
    print(sdv.nfl.NFLPlayProcess(gameId=401220403).espn_nfl_pbp())
    print(sdv.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp())
    print(sdv.nhl.espn_nhl_pbp(game_id=401247153))


if __name__ == "__main__":
    main()
