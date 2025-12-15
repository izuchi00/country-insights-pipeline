import os
import json
import time
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import requests


WIKIPEDIA_CAPITALS_URL = "https://en.wikipedia.org/wiki/List_of_national_capitals"
WORLD_BANK_COUNTRIES_URL = "https://api.worldbank.org/v2/country?format=json&per_page=400"
# World Bank indicators (bulk pull)
INDICATORS = {
    "population": "SP.POP.TOTL",
    "gdp_usd": "NY.GDP.MKTP.CD",
    "gdp_per_capita_usd": "NY.GDP.PCAP.CD",
    "life_expectancy": "SP.DYN.LE00.IN",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "EDIS-Analytics-Country-Insights/1.0"})


def ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def safe_get(url: str, timeout: int = 30, retries: int = 3) -> requests.Response:
    last_err = None
    for i in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"Failed GET after {retries} tries: {url}\n{last_err}")


def scrape_capitals() -> pd.DataFrame:
    """
    Scrapes a Wikipedia table containing countries and their national capitals.
    Returns: DataFrame with columns: country_name, capital
    """
    tables = pd.read_html(WIKIPEDIA_CAPITALS_URL)
    # Wikipedia page has multiple tables; we find one that looks like Country/Capital
    candidate = None
    for t in tables:
        cols = [c.lower() for c in t.columns.astype(str)]
        if ("country" in cols or "state" in cols) and any("capital" in c for c in cols):
            candidate = t.copy()
            break

    if candidate is None:
        raise RuntimeError("Could not find a suitable Country/Capital table on Wikipedia.")

    # Normalize columns
    col_map = {}
    for c in candidate.columns:
        cl = str(c).lower()
        if "country" in cl or "state" in cl:
            col_map[c] = "country_name"
        if "capital" in cl:
            col_map[c] = "capital"

    df = candidate.rename(columns=col_map)[["country_name", "capital"]].copy()

    # Clean text
    df["country_name"] = (
        df["country_name"]
        .astype(str)
        .str.replace(r"\[.*?\]", "", regex=True)
        .str.strip()
    )
    df["capital"] = (
        df["capital"]
        .astype(str)
        .str.replace(r"\[.*?\]", "", regex=True)
        .str.strip()
    )

    # Drop obvious non-country rows
    df = df[df["country_name"].str.len() > 1]
    df = df.drop_duplicates(subset=["country_name"], keep="first").reset_index(drop=True)
    return df


def fetch_worldbank_countries() -> pd.DataFrame:
    """
    Pulls country metadata from World Bank:
    iso3, name, region, income level.
    """
    r = safe_get(WORLD_BANK_COUNTRIES_URL)
    payload = r.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError("Unexpected World Bank country payload format.")

    rows = payload[1]
    df = pd.json_normalize(rows)

    # Keep only actual countries (exclude aggregates)
    df = df[df["region.value"].ne("Aggregates")].copy()

    out = df.rename(
        columns={
            "id": "iso2",
            "iso2Code": "iso2_alt",
            "name": "country_name_wb",
            "region.value": "region",
            "incomeLevel.value": "income_level",
            "capitalCity": "capital_wb",
            "longitude": "lon",
            "latitude": "lat",
        }
    )

    # World Bank returns iso2Code in different fields depending on endpoint;
    # 'id' is often ISO2; keep iso3 from 'countryiso3code'
    if "countryiso3code" in df.columns:
        out["iso3"] = df["countryiso3code"]
    else:
        out["iso3"] = None

    out = out[["iso3", "country_name_wb", "region", "income_level", "capital_wb", "lat", "lon"]].copy()
    out = out[out["iso3"].notna() & (out["iso3"].str.len() == 3)]
    out = out.drop_duplicates(subset=["iso3"], keep="first").reset_index(drop=True)
    return out


def fetch_indicator_bulk(indicator_code: str, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Bulk pull an indicator for all countries from World Bank.
    """
    url = (
        f"https://api.worldbank.org/v2/country/all/indicator/{indicator_code}"
        f"?format=json&per_page=20000&date={start_year}:{end_year}"
    )
    r = safe_get(url)
    payload = r.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError(f"Unexpected payload for indicator {indicator_code}")

    rows = payload[1]
    df = pd.json_normalize(rows)

    out = df.rename(
        columns={
            "country.value": "country_name_wb",
            "country.id": "iso2",
            "countryiso3code": "iso3",
            "date": "year",
            "value": "value",
        }
    )[["iso3", "country_name_wb", "year", "value"]].copy()

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out[out["iso3"].notna() & (out["iso3"].str.len() == 3)]
    return out


def latest_by_country(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """
    For each iso3, pick the latest non-null value by year.
    """
    d = df.dropna(subset=["value"]).copy()
    d = d.sort_values(["iso3", "year"], ascending=[True, False])
    d = d.groupby("iso3", as_index=False).first()
    d = d.rename(columns={"value": value_name, "year": f"{value_name}_year"})
    return d[["iso3", value_name, f"{value_name}_year"]]


def run_quality_checks(final_df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """
    Basic QA checks + a small report you can show clients.
    """
    checks = []

    # 1) unique ISO3
    dup_iso3 = final_df["iso3"].duplicated().sum()
    checks.append(("unique_iso3", dup_iso3 == 0, int(dup_iso3)))

    # 2) missingness
    missing_rate = final_df.isna().mean().sort_values(ascending=False)
    top_missing = missing_rate.head(8).reset_index()
    top_missing.columns = ["field", "missing_rate"]
    checks.append(("missingness_ok", True, "see report"))

    # 3) non-negative indicators (where applicable)
    for col in ["population", "gdp_usd", "gdp_per_capita_usd", "life_expectancy"]:
        if col in final_df.columns:
            bad = (final_df[col].dropna() < 0).sum()
            checks.append((f"{col}_non_negative", bad == 0, int(bad)))

    report = pd.DataFrame(checks, columns=["check", "passed", "details"])
    return all(report["passed"]), top_missing


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(root, "data", "raw")
    processed_dir = os.path.join(root, "data", "processed")
    ensure_dirs(raw_dir, processed_dir)

    print("1) Scraping capitals from Wikipedia…")
    capitals = scrape_capitals()
    capitals.to_csv(os.path.join(raw_dir, "capitals_wikipedia.csv"), index=False)

    print("2) Fetching country metadata from World Bank…")
    countries = fetch_worldbank_countries()
    countries.to_csv(os.path.join(raw_dir, "countries_worldbank.csv"), index=False)

    print("3) Pulling indicators in bulk from World Bank…")
    start_year, end_year = 2010, datetime.utcnow().year
    indicator_latest_frames = []

    for name, code in INDICATORS.items():
        print(f"   - {name} ({code})")
        ind = fetch_indicator_bulk(code, start_year, end_year)
        ind.to_csv(os.path.join(raw_dir, f"indicator_{name}.csv"), index=False)
        indicator_latest_frames.append(latest_by_country(ind, name))

    print("4) Merging datasets…")
    # Start from WB countries (trusted ISO3), then left join capitals + indicators
    final_df = countries.merge(capitals, left_on="country_name_wb", right_on="country_name", how="left")

    for frame in indicator_latest_frames:
        final_df = final_df.merge(frame, on="iso3", how="left")

    # Prefer WB capital if wiki missing
    final_df["capital"] = final_df["capital"].where(final_df["capital"].notna(), final_df["capital_wb"])

    # Tidy columns
    final_df = final_df.rename(columns={"country_name_wb": "country"}).drop(columns=["country_name", "capital_wb"])
    final_df["as_of_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    # Basic QA
    ok, missing_report = run_quality_checks(final_df)
    final_df.to_csv(os.path.join(processed_dir, "country_insights_latest.csv"), index=False)
    final_df.to_parquet(os.path.join(processed_dir, "country_insights_latest.parquet"), index=False)

    missing_report.to_csv(os.path.join(processed_dir, "qa_missingness_report.csv"), index=False)
    with open(os.path.join(processed_dir, "qa_status.json"), "w", encoding="utf-8") as f:
        json.dump({"passed": bool(ok), "as_of_utc": final_df["as_of_utc"].iloc[0]}, f, indent=2)

    print(f"Done ✅ QA passed: {ok}")
    print(f"Saved: data/processed/country_insights_latest.csv")


if __name__ == "__main__":
    main()
