"""
ESEF Filing Pipeline
====================
Downloads new ESEF filings from filings.xbrl.org, processes them through
loader.py (XBRL facts + narrative text sections), and saves outputs to
Google Drive.

Run:
    python pipeline.py                  # normal daily run (up to 5 new filings)
    python pipeline.py --limit 10       # process up to 10
    python pipeline.py --dry-run        # show what would be processed, no downloads
    python pipeline.py --watchlist-only # only process watchlist companies

Requirements:
    pip install requests google-auth google-auth-httplib2 google-api-python-client
    beautifulsoup4, arelle-release, pandas  (already in requirements.txt)

Google Drive setup:
    Set GDRIVE_CREDENTIALS env var to the contents of your service account JSON.
    Set GDRIVE_FOLDER_ID env var to the ID of the target Drive folder.
"""

import argparse
import io
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── Config ────────────────────────────────────────────────────────────────────

FILINGS_API   = "https://filings.xbrl.org/api/filings"
STATE_FILE    = Path(__file__).parent / "pipeline_state.json"
WATCHLIST_FILE = Path(__file__).parent / "watchlist.json"
MAX_NEW_PER_RUN = 5          # catch-all UK filings cap per day
REQUEST_DELAY   = 2.0        # seconds between API calls (be polite)


# ── State management ──────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"processed": [], "last_run": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))
    log.info(f"State saved — {len(state['processed'])} filings recorded")


def already_processed(filing_id: str, state: dict) -> bool:
    return filing_id in state["processed"]


def mark_processed(filing_id: str, state: dict):
    if filing_id not in state["processed"]:
        state["processed"].append(filing_id)


# ── Watchlist ─────────────────────────────────────────────────────────────────

def load_watchlist() -> list[dict]:
    """
    Returns list of dicts with at least 'lei' and optionally 'name'.
    Example watchlist.json:
    [
        {"lei": "2138001YYBULX5SZ2H24", "name": "Schroders"},
        {"lei": "213800MBWEIJDM5CU638", "name": "HSBC"}
    ]
    """
    if not WATCHLIST_FILE.exists():
        log.warning("No watchlist.json found — skipping watchlist pass")
        return []
    data = json.loads(WATCHLIST_FILE.read_text())
    log.info(f"Watchlist loaded: {len(data)} companies")
    return data


# ── filings.xbrl.org API ──────────────────────────────────────────────────────

def fetch_filings_for_lei(lei: str) -> list[dict]:
    """Fetch latest filings for a specific LEI."""
    params = {
        "filter[entity.identifier]": lei,
        "sort": "-period_end",
        "page[size]": 5,
    }
    try:
        r = requests.get(FILINGS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("data", [])
    except Exception as e:
        log.warning(f"API error fetching LEI {lei}: {e}")
        return []


def fetch_new_uk_filings(limit: int) -> list[dict]:
    """
    Fetch recent UK filings. Relies on pipeline_state.json to skip
    already-processed filings rather than date filtering (avoids API
    parameter compatibility issues).
    """
    params = {
        "filter[country]": "GB",
        "sort": "-period_end",
        "page[size]": min(limit * 4, 100),
    }
    try:
        r = requests.get(FILINGS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("data", [])
    except Exception as e:
        log.warning(f"API error fetching UK filings: {e}")
        return []


FILINGS_BASE = "https://filings.xbrl.org"

def get_zip_url(filing: dict) -> str | None:
    """Extract the package ZIP download URL from a filing record."""
    def fix_url(url: str) -> str:
        """Prepend base URL if the path is relative."""
        if url and not url.startswith("http"):
            return FILINGS_BASE + ("" if url.startswith("/") else "/") + url
        return url

    try:
        attrs = filing.get("attributes", {})
        # Try package_url attribute
        if attrs.get("package_url"):
            return fix_url(attrs["package_url"])
        # Try links object
        links = filing.get("links", {})
        if links.get("package"):
            return fix_url(links["package"])
        # Try relationships
        rels = filing.get("relationships", {})
        for rel_name in ("filing_index", "report_package", "zip"):
            if rel_name in rels:
                url = rels[rel_name].get("links", {}).get("related")
                if url:
                    return fix_url(url)
        # Last resort: construct URL from filing ID pattern
        # e.g. /LEI/PERIOD/ESEF/GB/0/LEI-PERIOD.zip
        fid = filing.get("id", "")
        if fid:
            constructed = f"{FILINGS_BASE}/{fid}.zip"
            return constructed
    except Exception:
        pass
    return None


def get_filing_meta(filing: dict) -> dict:
    """Extract key metadata from a filing record."""
    attrs = filing.get("attributes", {})
    entity = filing.get("relationships", {}).get("entity", {})
    return {
        "filing_id":   filing.get("id", "unknown"),
        "entity_name": attrs.get("entity_name") or attrs.get("name", "Unknown"),
        "lei":         attrs.get("lei", ""),
        "period_end":  attrs.get("period_end", ""),
        "added_time":  attrs.get("added_time", ""),
        "country":     attrs.get("country", "GB"),
    }


# ── Download ──────────────────────────────────────────────────────────────────

def download_zip(url: str) -> bytes | None:
    """Download a ZIP file, return bytes or None on failure."""
    try:
        log.info(f"  Downloading: {url}")
        r = requests.get(url, timeout=120, stream=True)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log.warning(f"  Download failed: {e}")
        return None


# ── Processing ────────────────────────────────────────────────────────────────

def process_filing(zip_bytes: bytes, meta: dict) -> tuple[bytes | None, bytes | None]:
    """
    Run zip_bytes through loader.py.
    Returns (facts_csv_bytes, text_csv_bytes). Either may be None on failure.
    """
    # Add parent dir to path so we can import loader
    repo_root = Path(__file__).parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from loader import load_facts_from_file, load_text_sections
    except ImportError as e:
        log.error(f"  Cannot import loader.py: {e}")
        return None, None

    facts_csv = None
    text_csv  = None

    # ── XBRL facts ────────────────────────────────────────────────────────────
    try:
        df, logs, _ = load_facts_from_file(zip_bytes, "zip", meta["filing_id"] + ".zip")
        if not df.empty:
            # Add filing metadata columns
            for col, val in [
                ("entity_name", meta["entity_name"]),
                ("lei",         meta["lei"]),
                ("period_end",  meta["period_end"]),
                ("filing_id",   meta["filing_id"]),
            ]:
                df.insert(0, col, val)
            facts_csv = df.to_csv(index=False).encode("utf-8")
            log.info(f"  Facts: {len(df):,} rows extracted")
        else:
            log.warning("  Facts: no rows extracted")
        for line in logs:
            log.debug(f"  [arelle] {line}")
    except Exception as e:
        log.warning(f"  Facts extraction failed: {e}")

    # ── Text sections ─────────────────────────────────────────────────────────
    try:
        text_df = load_text_sections(zip_bytes, "zip", meta["filing_id"] + ".zip")
        if not text_df.empty:
            for col, val in [
                ("entity_name", meta["entity_name"]),
                ("lei",         meta["lei"]),
                ("period_end",  meta["period_end"]),
                ("filing_id",   meta["filing_id"]),
            ]:
                text_df.insert(0, col, val)
            text_csv = text_df.to_csv(index=False).encode("utf-8")
            log.info(f"  Text: {len(text_df):,} chunks extracted")
        else:
            log.warning("  Text: no chunks extracted")
    except Exception as e:
        log.warning(f"  Text extraction failed: {e}")

    return facts_csv, text_csv


# ── Google Drive ──────────────────────────────────────────────────────────────

def get_drive_service():
    """Build a Google Drive service from GDRIVE_CREDENTIALS env var."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_json = os.environ.get("GDRIVE_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GDRIVE_CREDENTIALS env var not set")

    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def ensure_drive_folder(service, name: str, parent_id: str) -> str:
    """Get or create a subfolder by name under parent_id. Returns folder ID."""
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    # Create it
    meta = {
        "name":     name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents":  [parent_id],
    }
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def upload_to_drive(service, filename: str, csv_bytes: bytes, folder_id: str):
    """Upload a CSV file to Drive, overwriting if it already exists."""
    from googleapiclient.http import MediaIoBaseUpload

    # Check if file already exists
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    existing = results.get("files", [])

    media = MediaIoBaseUpload(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        resumable=False,
    )

    if existing:
        service.files().update(
            fileId=existing[0]["id"],
            media_body=media,
        ).execute()
        log.info(f"  Drive: updated {filename}")
    else:
        file_meta = {"name": filename, "parents": [folder_id]}
        service.files().create(
            body=file_meta,
            media_body=media,
            fields="id",
        ).execute()
        log.info(f"  Drive: uploaded {filename}")


def save_outputs_to_drive(
    service,
    root_folder_id: str,
    meta: dict,
    facts_csv: bytes | None,
    text_csv: bytes | None,
):
    """
    Save outputs to Drive under:
      root/
        {entity_name} ({period_end})/
          facts.csv
          text_sections.csv
    """
    safe_name = meta["entity_name"].replace("/", "-").replace("\\", "-")[:50]
    period    = meta["period_end"][:10] if meta["period_end"] else "unknown"
    folder_name = f"{safe_name} ({period})"

    company_folder_id = ensure_drive_folder(service, folder_name, root_folder_id)
    log.info(f"  Drive folder: {folder_name}")

    if facts_csv:
        upload_to_drive(service, "facts.csv", facts_csv, company_folder_id)
    if text_csv:
        upload_to_drive(service, "text_sections.csv", text_csv, company_folder_id)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(limit: int = MAX_NEW_PER_RUN, dry_run: bool = False, watchlist_only: bool = False):
    log.info("=" * 60)
    log.info(f"ESEF Pipeline starting  —  {datetime.now(timezone.utc).isoformat()[:19]}Z")
    log.info(f"dry_run={dry_run}  watchlist_only={watchlist_only}  limit={limit}")
    log.info("=" * 60)

    state        = load_state()
    watchlist    = load_watchlist()
    root_folder  = os.environ.get("GDRIVE_FOLDER_ID")
    drive_service = None

    if not dry_run:
        if not root_folder:
            log.error("GDRIVE_FOLDER_ID env var not set — cannot save outputs")
            sys.exit(1)
        try:
            drive_service = get_drive_service()
            log.info("Google Drive connected ✓")
        except Exception as e:
            log.error(f"Google Drive connection failed: {e}")
            sys.exit(1)

    processed_this_run = 0

    # ── Pass 1: Watchlist ─────────────────────────────────────────────────────
    log.info(f"\n── Pass 1: Watchlist ({len(watchlist)} companies) ──")
    for company in watchlist:
        lei  = company["lei"]
        name = company.get("name", lei)
        log.info(f"\nFetching filings for {name} ({lei})")
        filings = fetch_filings_for_lei(lei)
        time.sleep(REQUEST_DELAY)

        for filing in filings[:2]:   # latest 2 per watchlist company
            meta = get_filing_meta(filing)
            fid  = meta["filing_id"]

            if already_processed(fid, state):
                log.info(f"  Already processed: {fid}")
                continue

            log.info(f"  New filing: {fid}  period={meta['period_end']}")
            if dry_run:
                log.info("  [dry-run] skipping download/processing")
                continue

            zip_url = get_zip_url(filing)
            if not zip_url:
                log.warning(f"  No ZIP URL found for {fid}")
                continue

            zip_bytes = download_zip(zip_url)
            if not zip_bytes:
                continue
            time.sleep(REQUEST_DELAY)

            log.info(f"  Processing {name} ({meta['period_end']})…")
            facts_csv, text_csv = process_filing(zip_bytes, meta)
            save_outputs_to_drive(drive_service, root_folder, meta, facts_csv, text_csv)
            mark_processed(fid, state)
            processed_this_run += 1

    # ── Pass 2: New UK filings (catch-all) ────────────────────────────────────
    if not watchlist_only:
        remaining = limit - processed_this_run
        log.info(f"\n── Pass 2: New UK filings (up to {remaining} more) ──")

        filings = fetch_new_uk_filings(limit=remaining)
        time.sleep(REQUEST_DELAY)

        new_count = 0
        for filing in filings:
            if new_count >= remaining:
                break

            meta = get_filing_meta(filing)
            fid  = meta["filing_id"]

            if already_processed(fid, state):
                continue

            # Skip companies already handled in watchlist pass
            if any(w["lei"] == meta["lei"] for w in watchlist):
                continue

            log.info(f"\n  New UK filing: {meta['entity_name']}  {meta['period_end']}  ({fid})")
            if dry_run:
                log.info("  [dry-run] skipping download/processing")
                new_count += 1
                continue

            zip_url = get_zip_url(filing)
            if not zip_url:
                log.warning(f"  No ZIP URL for {fid}")
                continue

            zip_bytes = download_zip(zip_url)
            if not zip_bytes:
                continue
            time.sleep(REQUEST_DELAY)

            log.info(f"  Processing {meta['entity_name']}…")
            facts_csv, text_csv = process_filing(zip_bytes, meta)
            save_outputs_to_drive(drive_service, root_folder, meta, facts_csv, text_csv)
            mark_processed(fid, state)
            processed_this_run += 1
            new_count += 1

    # ── Wrap up ───────────────────────────────────────────────────────────────
    state["last_run"] = {
        "date":      datetime.now(timezone.utc).date().isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "processed_count": processed_this_run,
    }

    if not dry_run:
        save_state(state)

    log.info(f"\n── Done: {processed_this_run} filing(s) processed this run ──")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESEF Filing Pipeline")
    parser.add_argument("--limit",          type=int, default=MAX_NEW_PER_RUN,
                        help=f"Max new UK filings per run (default {MAX_NEW_PER_RUN})")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Show what would be processed without downloading")
    parser.add_argument("--watchlist-only", action="store_true",
                        help="Only process watchlist companies")
    args = parser.parse_args()
    run(limit=args.limit, dry_run=args.dry_run, watchlist_only=args.watchlist_only)
