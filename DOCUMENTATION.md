# Project Documentation — Canada–Brazil Trade Analytics

Complete technical and operational documentation for the Canada–Brazil bilateral trade dashboard and data pipeline built for the Federation of Canadian-Brazilian Businesses (FCBB).

This document has two audiences: the **team that maintains the project** (operations, publishing changes, troubleshooting) and **the author's thesis (TCC)**, for which the sections below map onto the usual structure (context, objectives, methodology, architecture, results, limitations, future work).

---

## 1. Context

Canada and Brazil trade billions of dollars in goods every year, but the official data is published by Statistics Canada in raw monthly archives that are hard to explore. FCBB — whose mission is to promote bilateral trade and investment — needed a way to turn that public data into an accessible, always-current view that helps its members identify opportunities and reduce the time spent on market research.

The project began as several disconnected repositories with a fragile, partly manual process (an external data store, secrets, and no guaranteed sequencing). It was consolidated into a **single, self-contained, fully automated pipeline** that runs in the cloud and updates itself every month with no human intervention.

## 2. Objectives & scope

- **Goal.** Convert public Statistics Canada data into a monthly, automated, cloud-based decision tool for Canada–Brazil trade, with a clean handoff to FCBB.
- **Decisions supported.** Directional decisions, not transactional ones: whether Canada is a market for a Brazilian product (and vice versa), which provinces concentrate demand, how the relationship is trending, and where it is likely heading short term.
- **Audience.** FCBB staff (advanced users), FCBB member companies (decision-makers), and the general public (the dashboard is embedded on the public FCBB site).
- **Update cadence.** Monthly — dictated by the source, which publishes once per month.
- **Out of scope (by data source).** Services trade, investment flows, company-level identities, tariffs/logistics costs, and real-time/weekly data are not available from CIMT.

## 3. System architecture

The entire system is one repository, mirrored to two GitHub accounts, driven by a single scheduled GitHub Actions job. There is **no external service and no secret** — the pipeline uses only GitHub's built-in `GITHUB_TOKEN`.

**Monthly job, in order:**

1. Recover the accumulated parquet from the GitHub Release asset `data-store`.
2. `extract.py` — download only the months not yet present (incremental).
3. `transform.py` — merge the new months into the parquet (merge, not overwrite; de-duplicated) and reconcile against Statistics Canada totals.
4. Persist the updated parquet back to the `data-store` Release.
5. `aggregate.py` — build the JSON/CSV datasets the dashboard reads (`site/data/*.json`, `site/data_csv/*.csv`).
6. `forecast.py` — compute the short-term forecast (`site/data/forecast.json`).
7. Commit the updated data files.
8. Publish the `site/` folder to GitHub Pages.

**Why the parquet lives in a Release, not in git:** it is large and changes every month. Storing it as a Release asset keeps the repository small and the history clean, while remaining fully in the cloud and requiring no external account.

See `docs/pipeline-diagram.svg` for the architecture figure.

## 4. Data source & methodology

- **Source.** Statistics Canada — Canadian International Merchandise Trade (CIMT), **customs basis**, values in **CAD**.
- **Temporal scope.** January 2019 onward, set by `START_YEAR` in `pipeline/extract.py`. Earlier years are excluded because the country-lookup file `ODPF_6_CtyDesc.TXT` is missing from the pre-2019 archives, which would leave those records without country names. For 2019+, 262 countries are mapped.
- **Reconciliation.** Every month, aggregated totals are reconciled against Statistics Canada references. The national import and export totals match with a gap of ≈ 0.

### 4.1 Key data-quality decisions

These decisions are the core of the project's correctness and are worth preserving verbatim, because each one corrects a real error found during development:

- **Completeness (the ~3 billion CAD fix).** Early cleaning logic dropped rows and undercounted the monthly total by roughly 3 billion CAD. The pipeline now **keeps** HS chapters 98 and 99 (low-value/special transactions), HS code 9901 (confidential + low-value trade), and rows whose country could not be resolved. Dropping any of these silently removes real trade value. Result: national reconciliation gap ≈ 0.
- **Total exports.** Exports use **Total exports** (domestic + re-exports), not domestic exports alone.
- **Provincial attribution.** Imports are attributed by **province of clearance**; exports by **province of origin** — two distinct Statistics Canada concepts. Validated against StatCan tables **12-10-0175** (imports, clearance) and **12-10-0173** (domestic exports, origin). Re-exports are not attributable to a province of origin and are labelled as such in the dashboard.
- **Country attribution.** Country names come from the official Statistics Canada description file (`ODPF_6_CtyDesc.TXT`), which is authoritative. A two-letter ISO code is used only for flags. This corrected a collision in which China was shown as Switzerland (ISO `CH` is Switzerland; China is `CN`).
- **Number formatting.** All chart axes and tooltips use an adaptive K/M/B/T formatter, avoiding both floating-point display artifacts (e.g. `0.9000000000000001B`) and fixed-unit distortions.

## 5. The pipeline in detail

| Script | Responsibility |
| --- | --- |
| `pipeline/extract.py` | Incremental download from CIMT. Reads the months already in the parquet and fetches only what is missing. Applies province of origin to exports and the country lookup (2019+). |
| `pipeline/transform.py` | Merges new months into the accumulated parquet (de-duplicated), and reconciles totals against Statistics Canada references. |
| `analytics/aggregate.py` | Reads the parquet and produces the dashboard datasets: monthly, countries, HS2 products, provinces, and the Brazil-focus views, as JSON (`site/data/`) and CSV (`site/data_csv/`). |
| `analytics/forecast.py` | Runs after `aggregate.py` (~1 min). Produces the short-term forecast (`site/data/forecast.json`). Resilient: if a series fails, it degrades to "low confidence" rather than breaking the build. |

## 6. Forecast module

- **Scope.** Bilateral Canada–Brazil only, three monthly series: imports (CA←BR), exports (CA→BR), and total. Horizon: 1 to 6 months.
- **Model.** SARIMAX (log "airline" specification, `statsmodels`). Prediction intervals at 80% and 95% are analytic, positive, and asymmetric (a consequence of the log transform); a defensive `max(0, ·)` clamp guards any future model.
- **COVID handling.** The training start is chosen by backtest: imports/total train from 2019 (more data helps); exports train from 2021 (excluding the 2020 shock lowers the exports MAPE from ~31.6% to ~18.7%).
- **Validation.** Rolling-origin backtest, MAPE by horizon, compared against a seasonal-naive baseline (same month last year). A series is only labelled **reliable** if it beats the baseline.

| Series | MAPE (avg) | Naive (avg) | Beats baseline | Label |
| --- | --- | --- | --- | --- |
| Imports | 13.5% | 16.0% | Yes | reliable |
| Total | 12.6% | 13.7% | Yes | reliable |
| Exports | 18.1% | 31.2% | Yes (larger error) | directional |

- **Why exports are shown as "directional," not hidden.** Exports are volatile (driven by a few large contracts). The model beats the baseline comfortably but with a larger absolute error, so exports are displayed with wide bands and an honest "directional / lower-confidence" label. They are **not omitted**, because Canadian exports to Brazil are exactly the flow FCBB most wants to grow — transparency is more valuable than a cleaner chart.
- **Output.** `site/data/forecast.json`: per series, recent history plus an array of `{month, forecast, lower80, upper80, lower95, upper95}`, and `metadata {model, trained_through, backtest_mape_by_horizon, generated_at}`.
- **UI.** The "Projection" tab renders a fan chart (history + central projection + shaded 80/95% bands + seasonal-naive reference). The series selector switches imports/total/exports, and the confidence badge, error headline, and MAPE-by-horizon all update with the selected series. A fixed assumptions line reads: *"Short-term projection · assumes no price/FX/policy shocks · recent months subject to revision by Statistics Canada."* The Projection tab is intentionally **not** affected by the global date filter.

## 7. Operations & maintenance

### 7.1 Publishing a change

1. Create a feature branch.
2. Push it to **both** remotes and open a PR in each:
   - `origin` = `WilgnerCH/canada-brazil-trade`
   - `fcbb` = `wilgnersantos-beep/canada-brazil-trade`
3. Merge to `master` in both repositories.

### 7.2 ⚠️ FCBB deploy-branch note (important)

The **WilgnerCH** site publishes from `master`. The **FCBB** live site currently publishes from the branch **`fix/country-iso-mapping`**, not from `master`. Therefore, after merging to `fcbb/master`, you must also **merge `master` into `fix/country-iso-mapping`** on the FCBB repo and trigger its deploy — otherwise the FCBB live site will not show the change.

This is known technical debt (see §9). Until it is normalized, every FCBB change must reach `fix/country-iso-mapping`.

### 7.3 Access & credentials

The pipeline itself needs no secret. Manual git operations (pushing, opening PRs) use the **locally authenticated GitHub account** on whichever machine runs them; that account must have **write access to both repositories**. There is no separate service account — pushes succeed only where the local credential has permission.

### 7.4 Running the pipeline

- **On demand:** *Actions → update-trade-data → Run workflow.*
- **First run** performs the full historical load (2019 onward); it can be long. Every run after that is incremental and fast.
- **Locally:** see the commands in the README.

### 7.5 Where the data lives

- The accumulated parquet is the GitHub **Release asset `data-store`** — do not delete this release.
- The dashboard datasets (`site/data/`, `site/data_csv/`) are committed to the repo and regenerated each run.

### 7.6 Troubleshooting

- **A push was denied.** The local account lacks write access to that remote — grant it (Settings → Collaborators) or authenticate as the account that owns the repo.
- **FCBB live site not updating.** The change reached `fcbb/master` but not `fix/country-iso-mapping` (see §7.2).
- **A forecast series shows "low confidence."** `forecast.py` degraded that series intentionally; the build is still healthy.

## 8. Known limitations

- **Merchandise only** — no services trade or investment flows.
- **Aggregate figures** — no company-level or buyer identities.
- **Monthly cadence** with revisions to recent months by Statistics Canada; no real-time data.
- **2019 onward** — earlier years lack country attribution.
- **Exports forecast is directional** — treat as trend, not a precise figure.

## 9. Technical debt & future work

- **Normalize the FCBB deploy branch.** Move the FCBB Pages source from `fix/country-iso-mapping` to `master`, so both repositories publish the same way. Before switching, confirm nothing lives only on that branch. This removes the extra publishing step and the main handoff risk.
- **Forecast v2.** Add external regressors (CAD/BRL exchange rate, a commodity-price index) to the SARIMAX model to improve accuracy, once the univariate version has proven stable.
- **Overview layout.** The province highlight and the Top-10 partners card share a row and feel a little tight; consider relocating the province card.

## 10. Repository map

```
.github/workflows/update.yml   Monthly workflow — the entire automation
pipeline/extract.py            Incremental download from Statistics Canada
pipeline/transform.py          Merge into the accumulated parquet (dedup + reconcile)
analytics/aggregate.py         Parquet → dashboard datasets (JSON + CSV)
analytics/forecast.py          Short-term SARIMAX forecast → forecast.json
site/index.html                Interactive dashboard
site/data/                     JSON datasets (incl. forecast.json)
site/data_csv/                 CSV mirror of the datasets
docs/                          Architecture diagrams and this documentation
requirements.txt               Python dependencies
LICENSE                        MIT
```

## 11. References

- Statistics Canada — Canadian International Merchandise Trade (CIMT).
- Statistics Canada table **12-10-0175** — imports by province of clearance.
- Statistics Canada table **12-10-0173** — domestic exports by province of origin.
- [Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence).

---

*Built by Wilgner Chayder for the Federation of Canadian-Brazilian Businesses (FCBB). Data © Statistics Canada. Code licensed under MIT.*
