# Amazon Advertising Automation Toolkit

A suite of Python automation tools built with Playwright to streamline Amazon Advertising campaign management workflows — from campaign creation and naming to keyword optimization and case management.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                     Excel Template (Input)                       │
│  Media Plan / ASIN Info / Account Info / Keyword Templates       │
└──────────────┬───────────────────────────────────┬───────────────┘
               │                                   │
               ▼                                   ▼
┌──────────────────────────┐    ┌──────────────────────────────────┐
│   Campaign Builder       │    │  ASIN Keyword Data Scraping      │
│   (Batch campaign        │    │  (Scrape keyword data from       │
│    creation via UI)      │    │   Amazon Superset dashboard)     │
└──────────┬───────────────┘    └──────────────┬───────────────────┘
           │                                   │
           ▼                                   ▼
┌──────────────────────────┐    ┌──────────────────────────────────┐
│  Campaign Namer          │    │  Keywords Analysis               │
│  Optimizer               │    │  (Scoring, ranking, report       │
│  (Rename + keyword/      │    │   generation per brand)          │
│   audience/bid/budget    │    └──────────────────────────────────┘
│   optimization)          │
└──────────────────────────┘
               │
               ▼
┌──────────────────────────┐
│   Case Creator           │
│   (Auto-fill & submit    │
│    optimization cases)   │
└──────────────────────────┘
```

## Modules

| Script | Description |
|--------|-------------|
| `Campaign Builder.py` | Reads Media Plan + ASIN info from Excel, then batch-creates campaigns on Amazon Advertising console via browser automation |
| `Campaign Namer.py` | Scans existing campaigns, matches them against Media Plan naming conventions, and renames them in bulk |
| `Campaign_Namer_Optimizer.py` | Unified pipeline: scan campaigns → compare with Media Plan → extract ASINs → keyword scoring & ranking → audience optimization → bid/budget adjustment → rename — all in one pass |
| `Case Creator.py` | Reads case info from Excel, auto-fills and submits optimization cases on Amazon Case Manager |
| `ASIN Keyword Data Scraping.py` | Scrapes ASIN keyword trend data from Amazon's internal Superset dashboard, exports per-country CSVs |
| `keywords_analysis.py` | Processes scraped keyword CSVs into structured analysis workbooks with scoring and categorization |
| `chrome_port_finder.py` | Utility: auto-detects Chrome remote debugging port (9222–9230) |
| `start_chrome.py` | Utility: launches Chrome with remote debugging enabled |

## Tech Stack

- **Python 3.10+**
- **Playwright** — browser automation (CDP connection to existing Chrome session)
- **Pandas / NumPy** — data processing and Excel I/O
- **SQLite (in-memory)** — fast data joins during campaign building

## Key Design Decisions

- **Reuse existing browser session**: All tools connect to an already-running Chrome instance via CDP (`chrome_port_finder.py`), preserving login state and avoiding re-authentication
- **Non-destructive**: Scripts never close the browser window — the page stays open after execution for manual verification
- **Keyword scoring engine**: Multi-factor ranking (Purchase Share × Funnel × Relevance × Exclusion × Search Weight) with competitor brand exclusion and cold-start fallback for system-recommended keywords
- **Audience optimization**: Automatic classification (advertised/similar/category) with relevance-based trimming for remarketing campaigns

## Setup

### Prerequisites
- Python 3.10+
- Google Chrome installed
- Required packages:

```bash
pip install playwright pandas numpy openpyxl
playwright install chromium
```

### Usage

1. **Start Chrome with debugging enabled:**
```bash
python Skills/start_chrome.py
```

2. **Log in** to Amazon Advertising console manually in the opened Chrome window.

3. **Run any automation script:**
```bash
# Create campaigns from Media Plan
python "Skills/Campaign Builder.py"

# Optimize existing campaigns (naming + keywords + bids)
python Skills/Campaign_Namer_Optimizer.py

# Batch-create optimization cases
python "Skills/Case Creator.py"

# Scrape keyword data
python "Skills/ASIN Keyword Data Scraping.py"
```

## Data Files

- `Excel Template/` — Input spreadsheets (Media Plan, ASIN info, Account info). Sample templates are included in the repository.
- `Reporting-keyword/` — Keyword analysis reports generated at runtime (initially empty)
- `browser_data/` — Chrome user profile data with login sessions (excluded from repo)

## Project Structure

```
├── Skills/
│   ├── Campaign Builder.py
│   ├── Campaign Namer.py
│   ├── Campaign_Namer_Optimizer.py
│   ├── Case Creator.py
│   ├── ASIN Keyword Data Scraping.py
│   ├── keywords_analysis.py
│   ├── chrome_port_finder.py
│   ├── start_chrome.py
│   └── campaign format.xlsx
├── Excel Template/
│   ├── Media Plan For Campaign Builder.xlsx
│   ├── ASIN_Input_Template For Campaign Builder.xlsx
│   ├── ASIN_Input_Template For Keywords.xlsx
│   └── Account information For Case Creator.xlsx
├── Reporting-keyword/          # Generated at runtime
├── docs/
│   └── ASIN_Keyword_System_Architecture.md
├── .gitignore
└── README.md
```

## License

This project is for portfolio demonstration purposes.
