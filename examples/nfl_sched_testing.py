import tempfile

import pandas as pd
import pyreadr

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def main():
    data = pd.DataFrame()
    with tempfile.TemporaryDirectory() as tempdirname:
        for i in range(2018, 2022):
            nfl_schedule_df = pyreadr.read_r(
                pyreadr.download_file(
                    f"https://raw.githubusercontent.com/nflverse/nflverse-pbp/master/schedules/sched_{i}.rds",
                    f"{tempdirname}/nfl_sched_{i}.rds",
                )
            )[None]
            nfl_season_sched = pd.DataFrame(nfl_schedule_df)
            data = pd.concat([data, nfl_season_sched], ignore_index=True)
    print(data)


if __name__ == "__main__":
    main()
