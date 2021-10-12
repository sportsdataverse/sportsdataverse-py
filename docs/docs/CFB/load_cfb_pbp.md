---
title: Load CFB PBP
sidebar_label: Load CFB PBP
sidebar_position: 2
---

### cfbfastR.cfb.load_cfb_pbp(seasons: List[int])
Load men’s college football play by play data going back to 2003

Example:

    cfb_df = cfbfastR.cfb.load_cfb_pbp(seasons=range(2003,2021))

Args:

    seasons (list): Used to define different seasons. 2003 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2003.