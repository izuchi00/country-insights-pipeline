# Country Insights Pipeline (Scrape + API → Clean → QA → Export)

A reproducible data pipeline that:
- Scrapes a public table of national capitals (Wikipedia)
- Pulls country indicators from the World Bank API (population, GDP, GDP per capita, life expectancy)
- Produces cleaned, analysis-ready outputs plus QA artifacts

## Outputs
Generated in `data/processed/`:
- `country_insights_latest.csv`
- `qa_missingness_report.csv`
- `qa_status.json`

## Run locally
```bash
pip install -r requirements.txt
python src/pipeline.py
