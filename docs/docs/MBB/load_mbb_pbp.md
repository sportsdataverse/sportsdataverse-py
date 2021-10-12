---
title: Load MBB PBP
sidebar_label: Load MBB PBP
---

### hoopR.mbb.load_mbb_pbp(seasons: List[int])
Load men’s college basketball play by play data going back to 2002

Example:

    mbb_df = hoopR.mbb.load_mbb_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.