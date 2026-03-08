# ESEF Filing Pipeline — Setup Guide

Automatically downloads new ESEF filings from filings.xbrl.org daily,
processes them (XBRL facts + narrative text sections), and saves outputs
to Google Drive.

---

## How it works

1. **06:00 UTC daily** — GitHub Actions triggers the pipeline
2. **Watchlist first** — checks `watchlist.json` for your named companies,
   fetches their latest filings
3. **UK catch-all** — fetches up to 5 new UK filings not yet processed
4. **Processes each filing** — runs `loader.py` to extract XBRL facts and
   narrative text sections (reusing the same code as the Streamlit app)
5. **Saves to Google Drive** — one folder per company/period:
   ```
   ESEF Filings/
     Schroders (2025-12-31)/
       facts.csv
       text_sections.csv
     HSBC Holdings (2025-12-31)/
       facts.csv
       text_sections.csv
   ```
6. **Commits state** — `pipeline_state.json` is updated in the repo so
   the pipeline never re-processes the same filing twice

---

## One-time setup

### 1. Add pipeline files to your repo

Copy these files into your existing `ttoomse-prog` repo:

```
pipeline.py
watchlist.json
.github/workflows/daily_pipeline.yml
```

The `pipeline.py` sits at the **root** of the repo (same level as `app.py`
and `loader.py`) so it can import `loader.py` directly.

### 2. Create a Google Drive service account

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (or use an existing one)
3. Enable the **Google Drive API**
4. Go to **IAM & Admin → Service Accounts** → Create service account
5. Give it any name (e.g. `esef-pipeline`)
6. Click the account → **Keys** tab → **Add Key** → JSON
7. Download the JSON file — this is your `GDRIVE_CREDENTIALS`

### 3. Share your Drive folder with the service account

1. Create a folder in Google Drive called `ESEF Filings` (or any name)
2. Right-click → Share → paste the service account email
   (looks like `esef-pipeline@your-project.iam.gserviceaccount.com`)
3. Give it **Editor** access
4. Copy the folder ID from the URL:
   `https://drive.google.com/drive/folders/`**`THIS_IS_THE_FOLDER_ID`**

### 4. Add GitHub Secrets

In your repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name        | Value                                          |
|--------------------|------------------------------------------------|
| `GDRIVE_CREDENTIALS` | The entire contents of the service account JSON file |
| `GDRIVE_FOLDER_ID`   | The Drive folder ID from step 3               |

### 5. Edit your watchlist

Edit `watchlist.json` to add the companies you want prioritised:

```json
[
  {"lei": "2138001YYBULX5SZ2H24", "name": "Schroders"},
  {"lei": "213800MBWEIJDM5CU638", "name": "HSBC Holdings"}
]
```

LEIs can be looked up at [gleif.org](https://www.gleif.org/en/lei-data/global-lei-index/lei-search).

### 6. Test with a dry run

Trigger manually from **Actions tab → Daily ESEF Pipeline → Run workflow**,
set `dry_run = true`. You'll see which filings it would have processed
without downloading anything.

---

## Running locally

```bash
# Normal run (up to 5 new filings)
GDRIVE_CREDENTIALS=$(cat service_account.json) \
GDRIVE_FOLDER_ID=your_folder_id \
python pipeline.py

# Dry run — see what would be processed
python pipeline.py --dry-run

# Process more filings
python pipeline.py --limit 20

# Only watchlist companies
python pipeline.py --watchlist-only
```

---

## Files

| File | Purpose |
|------|---------|
| `pipeline.py` | Main pipeline script |
| `watchlist.json` | Companies to prioritise (edit this) |
| `pipeline_state.json` | Auto-managed — tracks processed filings |
| `.github/workflows/daily_pipeline.yml` | GitHub Actions schedule |

---

## Adjusting the daily limit

Edit `MAX_NEW_PER_RUN` in `pipeline.py` (default: 5) or pass `--limit N`
at runtime. The watchlist companies don't count toward this limit.
