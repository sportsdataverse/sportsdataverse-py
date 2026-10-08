---
title: "NHL — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 6
description: "NHL — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Play-by-play processing

### nhl_pbp_disk {#nhl_pbp_disk}

`nhl_pbp_disk(game_id, path_to_json)`

Read a saved ESPN NHL play-by-play payload from disk.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | The ESPN game id; the file read is `{game_id}.json`. |
| `path_to_json` |  |  | The directory holding the saved payloads. |

**Returns**

The payload exactly as saved (the raw ESPN summary JSON), ready for `helper_nhl_pbp`.
