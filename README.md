# Country Insights Pipeline (Daily Refresh + QA)

A lightweight data engineering pipeline that builds an analysis-ready **country insights dataset** from public sources, with automated refresh and quality checks.

The pipeline produces consistent outputs (CSV + Parquet) and publishes QA artifacts on every run via GitHub Actions.

## What it produces

Outputs are written to `data/processed/`:

- `country_insights_latest.csv`
- `country_insights_latest.parquet`
- `qa_missingness_report.csv`
- `qa_status.json`

Raw extracts (for traceability) are written to `data/raw/`.

## Data sources

Primary source:
World Bank API (country metadata + indicators)

Optional enrichment:
Wikipedia (national capitals table). If unavailable, the pipeline still completes using the master country dataset.

## QA checks

The pipeline generates QA artifacts alongside the dataset, including:

- Duplicate key checks (ISO3 uniqueness)
- Missingness summary (top fields by missing rate)
- Basic validity checks for numeric indicators (non-negative where applicable)

## Run locally

```bash
pip install -r requirements.txt
python src/pipeline.py
