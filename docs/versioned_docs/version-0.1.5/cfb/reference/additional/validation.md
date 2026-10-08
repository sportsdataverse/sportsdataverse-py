---
title: "CFB — additional Python functions — Validation"
sidebar_label: "Validation"
sidebar_position: 15
description: "CFB — additional Python functions — Validation — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Validation

### check_box_invariants {#check_box_invariants}

`check_box_invariants(drive_summary: 'dict | None' = None, situational: 'dict | None' = None) -> 'list[str]'`

Every identity the two aggregates must satisfy; violations as strings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drive_summary` | `dict \| None` | `None` | the dict from `create_drive_summary`, or None. |
| `situational` | `dict \| None` | `None` | the dict from `create_situational_stats`, or None. |

**Returns**

one line per violation, `[]` when everything holds.

**Example**

```python
assert check_box_invariants(summary, stats) == []
```
