# MBB — additional Python functions — ESPN

> MBB — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package.

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Flatten one ESPN scoreboard event for the schedule frame, in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  | One element of the scoreboard payload's `events` list. |

**Returns**

The same event, modified: `competitions[0]` gains `home` / `away` team dicts (with `score`, `winner`, `currentRank`, `linescores`, `records`), `notes_type` / `notes_headline` and `broadcast_market` / `broadcast_name`, and loses `competitors`, `broadcasts`, `notes`, `odds`, `leaders` and the other nested blocks the schedule frame does not use.
