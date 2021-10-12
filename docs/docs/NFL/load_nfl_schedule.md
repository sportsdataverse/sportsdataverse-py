---
title: Load NFL Schedule
sidebar_label: Load NFL Schedule
---

### cfbfastR.nfl.load_nfl_schedule(seasons: List[int])
Load NFL schedule data

Example:

    nfl_df = cfbfastR.nfl.load_nfl_schedule(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


