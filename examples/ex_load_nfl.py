import sportsdataverse as sdv


def main():
    nfl_pbp_df = sdv.nfl.load_nfl_pbp(seasons=range(2015, 2021))
    print(nfl_pbp_df.head())
    nfl_player_stats_df = sdv.nfl.load_nfl_player_stats()
    print(nfl_player_stats_df.head())
    nfl_rosters_df = sdv.nfl.load_nfl_player_stats()
    print(nfl_rosters_df.head())
    nfl_schedules_df = sdv.nfl.load_nfl_schedule(seasons=range(2015, 2021))
    print(nfl_schedules_df.head())
    nfl_teams_df = sdv.nfl.load_nfl_teams()
    print(nfl_teams_df.head())


if __name__ == "__main__":
    main()
