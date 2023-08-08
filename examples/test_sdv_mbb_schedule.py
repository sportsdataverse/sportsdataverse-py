import pandas as pd

import sportsdataverse as sdv


def main():
    df = pd.Series(sdv.mbb.espn_mbb_calendar(season=2022).dateURL)
    print(df[-5:])
    schedules = pd.concat(list(map(lambda x: sdv.mbb.espn_mbb_schedule(dates=x), df[-5:])))
    # schedules.to_csv('espn_mbb_schedule.csv',   index=False)
    print(schedules.notes)
    # # print(sdv.mbb.espn_mbb_schedule(dates=2020))


if __name__ == "__main__":
    main()
