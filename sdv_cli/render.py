"""Rich renderables for the ``sdv`` commands.

Pure functions from the library's return values (dicts and lists) to Rich objects, so the same
renderers can later be reused inside full-screen apps. Nothing here imports ``sportsdataverse``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from rich import box
from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_HEX = re.compile(r"^[0-9a-fA-F]{6}$")


def _hex(value: Any) -> Optional[str]:
    """``"ba0c2f"`` -> ``"#ba0c2f"``; anything that is not a six-digit hex colour -> ``None``."""
    return f"#{value}" if isinstance(value, str) and _HEX.match(value) else None


def swatch(value: Any) -> Text:
    """A two-cell colour block followed by the hex code, or the raw value when it is not a colour."""
    colour = _hex(value)
    if colour is None:
        return Text(str(value or "—"), style="dim")
    return Text.assemble(("  ", f"on {colour}"), (f" {value}", "dim"))


def _link(item: Mapping[str, Any], rel: str) -> Optional[str]:
    for link in item.get("links") or []:
        if rel in (link.get("rel") or []):
            return link.get("href")
    return None


def _facts(rows: Iterable[tuple[str, Any]]) -> Table:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="dim", justify="right", no_wrap=True)
    grid.add_column(overflow="fold")
    for label, value in rows:
        if value not in (None, "", []):
            grid.add_row(label, value if isinstance(value, Text) else str(value))
    return grid


def team_card(team: Mapping[str, Any], league: str) -> Panel:
    """One ESPN team as a card bordered in the team's own colour."""
    colours = Text.assemble(swatch(team.get("color")), "  ", swatch(team.get("alternateColor")))
    clubhouse = _link(team, "clubhouse")
    logo = next((lg.get("href") for lg in team.get("logos") or [] if lg.get("href")), None)
    body = _facts(
        [
            ("id", Text(str(team.get("id")), style="bold cyan")),
            ("abbreviation", team.get("abbreviation")),
            ("location", team.get("location")),
            ("colours", colours if team.get("color") else None),
            ("clubhouse", Text(clubhouse, style="blue") if clubhouse else None),
            ("logo", Text(logo, style="blue") if logo else None),
        ]
    )
    return Panel(
        body,
        title=f"[bold]{team.get('displayName') or team.get('name') or '?'}[/]",
        subtitle=f"[dim]{league}[/]",
        title_align="left",
        border_style=_hex(team.get("color")) or "cyan",
        expand=False,
        padding=(0, 2),
    )


def team_table(teams: Sequence[Mapping[str, Any]], league: str) -> Table:
    table = _table(f"{len(teams)} {league} teams")
    table.add_column("id", justify="right", style="cyan", no_wrap=True)
    table.add_column("team")
    table.add_column("abbr", style="dim")
    table.add_column("colour", no_wrap=True)
    for t in teams:
        table.add_row(
            str(t.get("id")), str(t.get("displayName")), str(t.get("abbreviation") or ""), swatch(t.get("color"))
        )
    return table


def athlete_card(athlete: Mapping[str, Any], league: str) -> Panel:
    position = (athlete.get("position") or {}).get("abbreviation")
    body = _facts(
        [
            ("id", Text(str(athlete.get("id")), style="bold cyan")),
            ("team", athlete.get("team_display_name")),
            ("team id", athlete.get("team_id")),
            ("position", position),
            ("jersey", athlete.get("jersey")),
            ("height", athlete.get("displayHeight")),
            ("weight", athlete.get("displayWeight")),
            ("age", athlete.get("age")),
        ]
    )
    return Panel(
        body,
        title=f"[bold]{athlete.get('fullName') or athlete.get('displayName') or '?'}[/]",
        subtitle=f"[dim]{league}[/]",
        title_align="left",
        border_style="cyan",
        expand=False,
        padding=(0, 2),
    )


def athlete_table(athletes: Sequence[Mapping[str, Any]], league: str) -> Table:
    table = _table(f"{len(athletes)} {league} athletes")
    table.add_column("id", justify="right", style="cyan", no_wrap=True)
    table.add_column("name")
    table.add_column("pos", style="dim")
    table.add_column("#", justify="right", style="dim")
    table.add_column("team")
    for a in athletes:
        table.add_row(
            str(a.get("id")),
            str(a.get("fullName") or a.get("displayName")),
            str((a.get("position") or {}).get("abbreviation") or ""),
            str(a.get("jersey") or ""),
            str(a.get("team_display_name") or ""),
        )
    return table


def _side(event: Mapping[str, Any], home_away: str) -> Mapping[str, Any]:
    comp = (event.get("competitions") or [{}])[0]
    return next((c for c in comp.get("competitors") or [] if c.get("homeAway") == home_away), {})


def _status(event: Mapping[str, Any]) -> str:
    status = (event.get("status") or {}).get("type") or {}
    return str(status.get("shortDetail") or status.get("description") or "")


def event_table(events: Sequence[Mapping[str, Any]], league: str) -> Table:
    """Games as rows: id, away @ home with scores, status."""
    table = _table(f"{len(events)} {league} games" if len(events) != 1 else f"{league} game")
    table.add_column("id", justify="right", style="cyan", no_wrap=True)
    table.add_column("away")
    table.add_column("", justify="right")
    table.add_column("home")
    table.add_column("", justify="right")
    table.add_column("status", style="dim")
    for ev in events:
        away, home = _side(ev, "away"), _side(ev, "home")
        table.add_row(
            str(ev.get("id")),
            str((away.get("team") or {}).get("displayName") or ""),
            str(away.get("score") or ""),
            str((home.get("team") or {}).get("displayName") or ""),
            str(home.get("score") or ""),
            _status(ev),
        )
    return table


def function_listing(result: Any, search: Optional[str]) -> RenderableType:
    """``list_functions`` output: one name per line (grep-friendly), the search term highlighted."""

    def names(items: Iterable[str]) -> Text:
        text = Text("\n".join(items))
        if search:
            text.highlight_words([search], style="bold yellow", case_sensitive=False)
        return text

    if isinstance(result, list):
        return names(result)
    parts: List[RenderableType] = []
    for league in sorted(result):
        parts.append(Text.assemble((league, "bold cyan"), (f"  {len(result[league])}", "dim")))
        parts.append(names(f"  {n}" for n in result[league]))
    return Group(*parts)


def count_table(counts: Mapping[str, int]) -> Table:
    """``function_count`` output, largest league first, with a total."""
    table = _table(f"{len(counts)} leagues and providers")
    table.add_column("league")
    table.add_column("functions", justify="right")
    for league, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        table.add_row(league, f"{n:,}")
    table.add_section()
    table.add_row(Text("total", style="bold"), Text(f"{sum(counts.values()):,}", style="bold"))
    return table


def _bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024
    return f"{n} B"


def cache_table(stats: Mapping[str, Any]) -> Table:
    rows: Dict[str, Any] = dict(stats)
    if "disk_bytes" in rows:
        rows["disk_bytes"] = _bytes(int(rows["disk_bytes"]))
    return _facts([(k.replace("_", " "), v) for k, v in rows.items()])


def miss(what: str, name: str, suggestions: Sequence[str]) -> Text:
    """``sdv: no nba team matched 'x'`` plus did-you-mean suggestions."""
    text = Text.assemble(("sdv: ", "bold red"), f"no {what} matched {name!r}")
    if suggestions:
        text.append("\n     did you mean ", style="dim")
        text.append(" · ".join(suggestions), style="bold")
        text.append("?", style="dim")
    return text


def _table(title: str) -> Table:
    return Table(title=title, title_justify="left", title_style="dim", header_style="bold", box=box.SIMPLE_HEAD)
