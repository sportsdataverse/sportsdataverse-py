- **Generated wrappers:** a `bool_str` flag (ESPN `active=`, Fox `include_answers=`, MLS `is_live=`) raises `ValueError`
  for any string other than `true`/`false`/`1`/`0`; `"yes"` was sent as `"true"` and `""` as `"false"`.
- **Season loaders (generated, and `load_nfl_team_group_seasons`):** a fractional season (`[2024.5]`, which
  loaded 2024) raises `ValueError`, as does a bool season; `seasons=None` raises `ValueError` where it raised
  `TypeError`.
- **ESPN wrappers:** a collection as a path value raises `ValueError`, and a float id is sent as its integer (`"13"`,
  not `"13.0"`).
- **ESPN wrappers:** a keyword the wrapper does not take raises `TypeError` naming the wrapper, before any request.
