import sportsdataverse


def main():
    nhl_df = sportsdataverse.nhl.nhl_api_pbp(game_id=2021020079)
    print(nhl_df)
    nhl_sched_df = sportsdataverse.nhl.nhl_api_schedule(start_date="2021-08-01", end_date="2021-10-01")
    print(nhl_sched_df)
    nhl_sched_df.to_csv("../nhl_sched.csv", index=False)


if __name__ == "__main__":
    main()
