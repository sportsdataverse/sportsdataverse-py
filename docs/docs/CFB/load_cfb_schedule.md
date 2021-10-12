---
title: Load CFB Schedule
sidebar_label: Load CFB Schedule
sidebar_position: 5
---

### cfbfastR.cfb.load_cfb_schedule(seasons: List[int])
Load men’s college football schedule data

Example:

    cfb_df = cfbfastR.cfb.load_cfb_schedule(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


