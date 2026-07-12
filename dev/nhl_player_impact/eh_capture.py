"""Capture the Evolving Hockey skater RAPM (Phase 3 xG-RAPM, EV strength) + skater
GAR/WAR (Phase 6, all-situations season total) Single-Season tables for the 2024-25
regular season, authenticated with the account's own Pro Subscriber session, and save
them as local CSVs for ``build_eh_fixture.py`` to turn into the committed
``eh_skaters.parquet`` fixture.

Credentials are read from the environment (EH_USER / EH_PASS) at call time -- never
hardcoded, never printed, never written to any output file. Ephemeral/gitignored
dev-only script (playwright is NOT a project dependency -- run via
``uv run --with playwright``).

Run (bash, reading straight from ~/.Renviron without ever echoing the secret)::

    EH_USER=$(sed -n 's/^EVOLVING_HOCKEY_USER=//p' ~/.Renviron | sed -E 's/^"(.*)"$/\\1/') \\
    EH_PASS=$(sed -n 's/^EVOLVING_HOCKEY_PASS=//p' ~/.Renviron | sed -E 's/^"(.*)"$/\\1/') \\
    uv run --with playwright python dev/nhl_player_impact/eh_capture.py
"""

from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

CACHE = Path(__file__).parent / "_cache"
SEASON_LABEL = "20242025"  # 2024-25 regular season -- matches the pbp_sample fixture.


def _login(page: Page) -> None:
    user = os.environ["EH_USER"]
    pw = os.environ["EH_PASS"]
    page.goto("https://evolving-hockey.com/login/", wait_until="networkidle", timeout=60000)
    page.fill("#user_login", user)
    page.fill("#user_pass", pw)
    page.click("#wp-submit")
    page.wait_for_load_state("networkidle", timeout=60000)
    assert "Log Out" in page.content() or "My Account" in page.content(), "EH login did not succeed"


def _download(page: Page, out_name: str) -> Path:
    with page.expect_download(timeout=60000) as dl_info:
        page.click("text=Download")
    download = dl_info.value
    out_path = CACHE / out_name
    download.save_as(str(out_path))
    return out_path


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        _login(page)

        # --- Skater RAPM (Phase 3 oracle): EV strength (our internal comparison uses
        # strength_states=["5v5"] since EH's RAPM tool has no "All situations combined"
        # option -- RAPM is inherently strength-segmented). ---
        page.goto("https://evolving-hockey.com/stats/skater_rapm/", wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        page.select_option("#rapm_sk_season", label=SEASON_LABEL)
        page.wait_for_timeout(500)
        page.click("text=Submit")
        page.wait_for_timeout(4000)
        rapm_csv = _download(page, "eh_skater_rapm_2024_regular_ev.csv")
        print("saved:", rapm_csv)

        # --- Skater GAR/WAR (Phase 6 oracle): season-total WAR, all situations. ---
        page.goto("https://evolving-hockey.com/stats/skater_gar/", wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        page.select_option("#gar_sk_season", label=SEASON_LABEL)
        page.wait_for_timeout(500)
        page.click("text=Submit")
        page.wait_for_timeout(4000)
        gar_csv = _download(page, "eh_skater_gar_2024_regular.csv")
        print("saved:", gar_csv)

        browser.close()


if __name__ == "__main__":
    main()
