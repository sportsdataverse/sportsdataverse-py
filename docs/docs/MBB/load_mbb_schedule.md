---
title: Load MBB Schedule
sidebar_label: Load MBB Schedule
---

### hoopR.mbb.load_mbb_schedule(seasons: List[int])
Load men’s college basketball schedule data

Example:

    mbb_df = hoopR.mbb.load_mbb_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


