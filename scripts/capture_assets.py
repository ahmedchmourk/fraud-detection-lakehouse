"""
Render README visual assets with headless Chromium (Playwright).

  * assets/architecture_diagram.png  <- assets/src/architecture.html
  * assets/medallion_flow.png        <- assets/src/medallion_flow.html
  * assets/dashboard_preview.png     <- live Streamlit dashboard
  * assets/minio_buckets.png         <- live MinIO console object browser

Run while the stack is up (see README):
  docker run --rm --network fraud-lakehouse_default -v "$PWD":/w -w /w \
    mcr.microsoft.com/playwright/python:v1.55.0-noble \
    sh -c "pip install -q playwright==1.55.0 && python scripts/capture_assets.py"
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "assets"
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://dashboard:8501")
MINIO_CONSOLE_URL = os.getenv("MINIO_CONSOLE_URL", "http://minio:9001")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def render_diagrams(browser) -> None:
    page = browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=1.5)
    for src, out in (("architecture.html", "architecture_diagram.png"), ("medallion_flow.html", "medallion_flow.png")):
        page.goto((ASSETS / "src" / src).as_uri())
        page.locator(".canvas").screenshot(path=str(ASSETS / out))
        print(f"rendered {out}")
    page.close()


def capture_dashboard(browser) -> None:
    # Streamlit scrolls inside its own container, so use a tall viewport instead of full_page.
    page = browser.new_page(viewport={"width": 1600, "height": 1900}, device_scale_factor=1.25)
    page.goto(DASHBOARD_URL)
    page.get_by_text("Gold layer analytics").wait_for(timeout=60_000)
    page.locator(".js-plotly-plot").nth(3).wait_for(timeout=60_000)
    page.wait_for_timeout(8_000)  # let fragments refresh and charts settle
    page.screenshot(path=str(ASSETS / "dashboard_preview.png"))
    print("captured dashboard_preview.png")
    page.close()


def capture_minio(browser) -> None:
    page = browser.new_page(viewport={"width": 1500, "height": 820}, device_scale_factor=1.25)
    page.goto(MINIO_CONSOLE_URL)
    page.locator("#accessKey").fill(MINIO_USER)
    page.locator("#secretKey").fill(MINIO_PASSWORD)
    page.locator("button[type=submit]").click()
    page.wait_for_load_state("networkidle")
    page.goto(f"{MINIO_CONSOLE_URL}/browser/bronze/transactions%2F")
    page.wait_for_timeout(2_000)
    acknowledge = page.get_by_role("button", name="Acknowledge")
    if acknowledge.count():
        acknowledge.click()
    page.wait_for_timeout(2_000)
    page.screenshot(path=str(ASSETS / "minio_buckets.png"))
    print("captured minio_buckets.png")
    page.close()


def main() -> int:
    failures = 0
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for step in (render_diagrams, capture_dashboard, capture_minio):
            try:
                step(browser)
            except (PlaywrightTimeout, Exception) as exc:  # noqa: BLE001
                failures += 1
                print(f"{step.__name__} failed: {exc}", file=sys.stderr)
        browser.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
