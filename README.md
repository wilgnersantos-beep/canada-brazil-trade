# Canada–Brazil Trade Analytics

Interactive dashboard and automated data pipeline for **Canada–Brazil bilateral merchandise trade**, built for the **Federation of Canadian-Brazilian Businesses (FCBB)**. A single, self-contained repository that extracts official Statistics Canada data every month — with no servers to manage, no external services, and no manual steps.

[![Open the Dashboard](https://img.shields.io/badge/Open_the_Dashboard-1f7a4d?style=for-the-badge)](https://wilgnersantos-beep.github.io/canada-brazil-trade/)

**Live dashboard:** [FCBB (primary)](https://wilgnersantos-beep.github.io/canada-brazil-trade/) · [Author mirror](https://wilgnerch.github.io/canada-brazil-trade/)

![Data: Statistics Canada CIMT](https://img.shields.io/badge/Data-Statistics%20Canada%20CIMT-c1331f)
![Update: Monthly / Automated](https://img.shields.io/badge/Update-Monthly%20%C2%B7%20Automated-1f7a4d)
![License: MIT](https://img.shields.io/badge/License-MIT-blue)

> **Maintainers:** for how to publish a change, credentials, the deploy-branch note, and troubleshooting, see **[DOCUMENTATION.md](DOCUMENTATION.md)**.

---

## What it does

It turns public Statistics Canada trade data into a **decision tool** for FCBB and its members: where Canadian demand is concentrated, which products and provinces matter, how the Canada–Brazil relationship is evolving, and where it is likely heading over the next few months. The dashboard updates automatically each month and runs entirely in the cloud.

## Highlights

- **Automatic monthly updates** via GitHub Actions — no local machine required.
- **No external services and no secrets.** The accumulated dataset is stored as a GitHub Release asset; the pipeline uses only GitHub's built-in `GITHUB_TOKEN`.
- **Validated data.** Aggregates are reconciled against Statistics Canada reference totals, with a national gap of ≈ 0 in every month.
- **Short-term forecast.** A SARIMAX model projects bilateral trade 1–6 months ahead, with prediction bands and honest, backtested error labels.
- **Published via GitHub Pages** and embedded on [fcbb.org](https://fcbb.org/).

## Architecture

![Pipeline](docs/pipeline-diagram.svg)

A single scheduled job runs in order each month: recover the accumulated parquet from the Release → extract only the months not yet stored → merge into the parquet → persist it back to the Release → aggregate into the JSON/CSV the dashboard reads → compute the forecast → commit → publish `site/` to GitHub Pages.

```
Statistics Canada (CIMT)
        │  download (monthly ZIPs)
        ▼
   extract.py ─► transform.py ─► [ Parquet in GitHub Release "data-store" ]
                                          │
                                          ▼
                                    aggregate.py ─► site/data/*.json, site/data_csv/*.csv
                                          │
                                    forecast.py ─► site/data/forecast.json
                                          │
                                          ▼
                                    GitHub Pages ─► iframe on fcbb.org
```

## Data source & scope

- **Source:** Statistics Canada — Canadian International Merchandise Trade (CIMT), customs basis, in CAD.
- **Period:** 2019-01 onward, configurable via `START_YEAR` in `pipeline/extract.py`. Years 2014–2018 are excluded because the country-lookup file (`ODPF_6_CtyDesc.TXT`) is absent from the pre-2019 archives, which would leave those records without country names.
- **Provincial attribution:** imports by **province of clearance**; exports by **province of origin** — two distinct Statistics Canada concepts, each applied correctly.
- **Reconciliation:** national import and export totals match the Statistics Canada references (gap ≈ 0) across all months.

## What the dashboard answers

| Tab | Question it answers |
| --- | --- |
| Overview | How large and balanced is Canada's trade, who are the top partners, and which provinces lead? |
| Monthly trends | How are flows and the trade balance evolving month to month? |
| By country | What does Canada's bilateral trade with a given country look like? |
| Products (HS) | Which product categories drive imports and exports? |
| Brazil Focus | Where are the opportunities between Canada and Brazil (opportunity matrix, Canadian strengths, balance)? |
| By Province | Which provinces concentrate demand, and for which products? |
| Projection | Where is Canada–Brazil bilateral trade likely heading over the next 1–6 months? |

## Repository structure

```
.github/workflows/update.yml   Monthly workflow — the entire automation
pipeline/extract.py            Incremental download from Statistics Canada
pipeline/transform.py          Merge into the accumulated parquet (dedup)
analytics/aggregate.py         Parquet → dashboard datasets (JSON + CSV)
analytics/forecast.py          Short-term SARIMAX forecast → forecast.json
site/index.html                Interactive dashboard
site/data/                     JSON datasets the dashboard reads (incl. forecast.json)
site/data_csv/                 CSV mirror of the datasets
docs/                          Architecture diagrams and documentation
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
PARQUET_PATH=data/canada_trade_full.parquet python analytics/forecast.py
python -m http.server -d site 8000   # open http://localhost:8000
```

## Repositories

Mirrored across two repositories, each publishing its own GitHub Pages site:

- **Author:** [WilgnerCH/canada-brazil-trade](https://github.com/WilgnerCH/canada-brazil-trade)
- **FCBB:** [wilgnersantos-beep/canada-brazil-trade](https://github.com/wilgnersantos-beep/canada-brazil-trade)

Changes are pushed to both so the two sites stay in sync. See [DOCUMENTATION.md](DOCUMENTATION.md) for the exact publishing procedure.

## Credits & license

Built by Wilgner Chayder for the Federation of Canadian-Brazilian Businesses (FCBB). Data © Statistics Canada, used under the [Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence). Code released under the **MIT License** — see [LICENSE](LICENSE).
