# Canada–Brazil Trade Analytics

[![Open the Dashboard](https://img.shields.io/badge/%F0%9F%9A%80_Open_the_Dashboard-1f7a4d?style=for-the-badge)](https://wilgnersantos-beep.github.io/canada-brazil-trade/)
Interactive dashboard and automated data pipeline for **Canada–Brazil bilateral merchandise trade**, built for the **Federation of Canadian-Brazilian Businesses (FCBB)**. A single, self-contained repository that extracts official Statistics Canada data every month — with no servers to manage, no external services, and no manual steps.

**Live dashboard:** [Open the FCBB dashboard](https://wilgnersantos-beep.github.io/canada-brazil-trade/) · [Mirror](https://wilgnerch.github.io/canada-brazil-trade/)

![Data: Statistics Canada CIMT](https://img.shields.io/badge/Data-Statistics%20Canada%20CIMT-c1331f)
![Update: Monthly / Automated](https://img.shields.io/badge/Update-Monthly%20%C2%B7%20Automated-1f7a4d)
![License: MIT](https://img.shields.io/badge/License-MIT-blue)

---

## What it does

It turns public Statistics Canada trade data into a **decision tool** for FCBB and its members: where Canadian demand is concentrated, which products and provinces matter, and how the Canada–Brazil relationship is evolving over time. The dashboard updates automatically each month and runs entirely in the cloud.

## Highlights

- **Automatic monthly updates** via GitHub Actions — no local machine required.
- **No external services and no secrets.** The accumulated dataset is stored as a GitHub Release asset; the pipeline uses only GitHub's built-in `GITHUB_TOKEN`.
- **Validated data.** Aggregates are reconciled against Statistics Canada reference totals, with a national gap of ≈ 0 in every month.
- **Published via GitHub Pages** and embedded on [fcbb.org](https://fcbb.org/).

## Architecture

![Pipeline](docs/pipeline-diagram.svg)

A single scheduled job runs in order each month: recover the accumulated parquet from the Release → extract only the months not yet stored → merge into the parquet → persist it back to the Release → aggregate into the JSON/CSV the dashboard reads → commit → publish `site/` to GitHub Pages.

```
Statistics Canada (CIMT)
        │  download (monthly ZIPs)
        ▼
   extract.py ──► transform.py ──► [ Parquet in GitHub Release "data-store" ]
                                            │
                                            ▼
                                      aggregate.py ──► site/data/*.json
                                                       site/data_csv/*.csv
                                                            │
                                                            ▼
                                                     GitHub Pages ──► iframe on fcbb.org
```

## Data source & scope

- **Source:** Statistics Canada — Canadian International Merchandise Trade (CIMT), customs basis.
- **Period:** 2019-01 onward, configurable via `START_YEAR` in `pipeline/extract.py`. Years 2014–2018 are excluded because the country-lookup file (`ODPF_6_CtyDesc.TXT`) is absent from the pre-2019 archives, which would leave those records without country names.
- **Provincial attribution:** imports are reported by **province of clearance**; exports by **province of origin** — these are two distinct Statistics Canada concepts, and the pipeline applies each correctly.
- **Reconciliation:** national import and export totals match the Statistics Canada references (gap ≈ 0) across all months from 2019-01 to the latest available.

## What the dashboard answers

Six views, each tied to a concrete question:

| Tab | Question it answers |
| --- | --- |
| Overview | How large and balanced is Canada's trade, and who are the top partners? |
| Monthly trends | How are flows and the trade balance evolving month to month? |
| By country | What does Canada's bilateral trade with a given country look like? |
| Products (HS) | Which product categories drive imports and exports? |
| Brazil Focus | Where are the opportunities between Canada and Brazil (opportunity matrix, Canadian strengths, balance)? |
| By Province | Which provinces concentrate demand, and for which products? |

## Repository structure

```
.github/workflows/update.yml   Monthly workflow — the entire automation
pipeline/extract.py            Incremental download from Statistics Canada
pipeline/transform.py          Merge into the accumulated parquet (dedup)
analytics/aggregate.py         Parquet → dashboard datasets (JSON + CSV)
site/index.html                Interactive dashboard
site/data/                     JSON datasets the dashboard reads
site/data_csv/                 CSV mirror of the datasets
docs/pipeline-diagram.svg      Architecture diagram
requirements.txt               Python dependencies
```

## Update schedule

The workflow runs monthly on days **10–13** (Statistics Canada publishes CIMT on the 6th–9th). The job is **idempotent**: if there is no new month, it completes without changes, so scheduling several days in a row is a safe retry window rather than a source of duplicate commits.

## Setup

1. **Enable GitHub Pages:** *Settings → Pages → Build and deployment → Source: GitHub Actions.*
2. **Run once:** *Actions → update-trade-data → Run workflow.*

The first run performs the full historical load (2019 onward); every run after that is incremental and fast. Because there are no secrets, nothing else needs to be configured.

## Local development (optional)

```bash
pip install -r requirements.txt
PARQUET_PATH=data/canada_trade_full.parquet python pipeline/extract.py
PARQUET_PATH=data/canada_trade_full.parquet python pipeline/transform.py
PARQUET_PATH=data/canada_trade_full.parquet python analytics/aggregate.py
python -m http.server -d site 8000   # open http://localhost:8000
```

> A `DEMO=1` mode is available for testing the pipeline end to end with synthetic data (no internet required). Production runs use `DEMO=0` (real Statistics Canada data).

## Repositories

The project is mirrored across two repositories, each publishing its own GitHub Pages site:

- **Author:** [WilgnerCH/canada-brazil-trade](https://github.com/WilgnerCH/canada-brazil-trade)
- **FCBB:** [wilgnersantos-beep/canada-brazil-trade](https://github.com/wilgnersantos-beep/canada-brazil-trade)

Changes are pushed to both so the two sites stay in sync.

## Credits & license

Built by Wilgner Chayder for the Federation of Canadian-Brazilian Businesses (FCBB). Data © Statistics Canada, used under the terms of the [Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence). Code released under the **MIT License** — see [LICENSE](LICENSE).

