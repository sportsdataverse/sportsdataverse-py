import sportsdataverse as sdv

def main():
    print(sdv.wbb.wbb_pbp(game_id=401266534))
    print(sdv.wnba.wnba_pbp(game_id=401370395))
    print(sdv.mbb.mbb_pbp(game_id=401265031))
    print(sdv.nba.nba_pbp(game_id=401307514))
    print(sdv.nfl.NFLPlayProcess(gameId=401220403).nfl_pbp())
    print(sdv.cfb.CFBPlayProcess(gameId=401256137).cfb_pbp())
    print(sdv.nhl.nhl_pbp(game_id=401247153))


if __name__ == "__main__":
    main()
