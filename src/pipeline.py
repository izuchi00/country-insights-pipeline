import os
import json
import time
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any

import pandas as pd
import requests


WORLD_BANK_COUNTRIES_BASE = "https://api.worldbank.org/v2/country?format=json"
WORLD_BANK_INDICATOR_BASE = "https://api.worldbank.org/v2/country/all/indicator/{code}?format=json&date={start}:{end}"
RESTCOUNTRIES_URL = (
    "https://restcountries.com/v3.1/all"
    "?fields=name,cca3,region,subregion,capital,latlng,population"
)

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


def safe_get(url: str, timeout: int = 30, retries: int = 6) -> requests.Response:
    last_err = None
    for i in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(60, 2 ** i))
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            last_err = e
            time.sleep(min(60, 2 ** i))

    raise RuntimeError(f"Failed GET after {retries} tries: {url}\n{last_err}")


def worldbank_fetch_all_pages(base_url: str, per_page: int = 100) -> List[Dict[str, Any]]:
    page = 1
    rows_all: List[Dict[str, Any]] = []

    while True:
        url = f"{base_url}&per_page={per_page}&page={page}"
        r = safe_get(url)
        payload = r.json()

        if not isinstance(payload, list) or len(payload) < 2:
            break

        meta = payload[0] or {}
        rows = payload[1] or []
        rows_all.extend(rows)

        pages = int(meta.get("pages", 0) or 0)
        if pages and page >= pages:
            break

        if not pages and not rows:
            break

        page += 1

    return rows_all


def fetch_worldbank_countries() -> pd.DataFrame:
    rows = worldbank_fetch_all_pages(WORLD_BANK_COUNTRIES_BASE, per_page=100)
    df = pd.json_normalize(rows)

    if df.empty:
        return pd.DataFrame(columns=["iso3", "country_name_wb", "region", "income_level", "capital_wb"])

    if "region.value" in df.columns:
        df = df[df["region.value"].ne("Aggregates")].copy()

    out = df.rename(
        columns={
            "name": "country_name_wb",
            "region.value": "region",
            "incomeLevel.value": "income_level",
            "capitalCity": "capital_wb",
            "longitude": "lon",
            "latitude": "lat",
        }
    )

    iso3_series = None
    for col in ("countryiso3code", "iso3Code", "iso3code"):
        if col in df.columns:
            iso3_series = df[col]
            break

    out["iso3"] = iso3_series if iso3_series is not None else None

    keep_cols = ["iso3", "country_name_wb", "region", "income_level", "capital_wb", "lat", "lon"]
    out = out[[c for c in keep_cols if c in out.columns]].copy()

    out["iso3"] = out["iso3"].astype(str).str.strip()
    out = out[out["iso3"].notna() & (out["iso3"].str.len() == 3)]
    out = out.drop_duplicates(subset=["iso3"], keep="first").reset_index(drop=True)

    return out


def fetch_restcountries() -> pd.DataFrame:
    r = safe_get(RESTCOUNTRIES_URL)
    rows = r.json()
    df = pd.json_normalize(rows)

    out = pd.DataFrame()
    out["iso3"] = df.get("cca3")
    out["country"] = df.get("name.common")
    out["region"] = df.get("region")
    out["subregion"] = df.get("subregion")

    capital = df.get("capital")
    if capital is not None:
        out["capital"] = capital.apply(lambda x: x[0] if isinstance(x, list) and len(x) else None)
    else:
        out["capital"] = None

    latlng = df.get("latlng")
    if latlng is not None:
        out["lat"] = latlng.apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        out["lon"] = latlng.apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
    else:
        out["lat"] = None
        out["lon"] = None

    out["population"] = pd.to_numeric(df.get("population"), errors="coerce")

    out["iso3"] = out["iso3"].astype(str).str.strip()
    out = out[out["iso3"].notna() & (out["iso3"].str.len() == 3)]
    out = out.drop_duplicates(subset=["iso3"], keep="first").reset_index(drop=True)

    return out


def fetch_countries_master() -> pd.DataFrame:
    try:
        wb = fetch_worldbank_countries()
        if not wb.empty:
            wb = wb.rename(columns={"country_name_wb": "country", "capital_wb": "capital"})
            return wb
    except Exception:
        pass

    rc = fetch_restcountries()
    return rc


def fetch_indicator_bulk(indicator_code: str, start_year: int, end_year: int) -> pd.DataFrame:
    base = WORLD_BANK_INDICATOR_BASE.format(code=indicator_code, start=start_year, end=end_year)
    rows = worldbank_fetch_all_pages(base, per_page=1000)
    df = pd.json_normalize(rows)

    if df.empty:
        return pd.DataFrame(columns=["iso3", "year", "value"])

    out = df.rename(columns={"countryiso3code": "iso3", "date": "year", "value": "value"})[
        ["iso3", "year", "value"]
    ].copy()

    out["iso3"] = out["iso3"].astype(str).str.strip()
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out[out["iso3"].notna() & (out["iso3"].str.len() == 3)]

    return out


def latest_by_country(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    d = df.dropna(subset=["value"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["iso3", value_name, f"{value_name}_year"])

    d = d.sort_values(["iso3", "year"], ascending=[True, False])
    d = d.groupby("iso3", as_index=False).first()
    d = d.rename(columns={"value": value_name, "year": f"{value_name}_year"})
    return d[["iso3", value_name, f"{value_name}_year"]]


def run_quality_checks(final_df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    checks = []

    dup_iso3 = final_df["iso3"].duplicated().sum() if "iso3" in final_df.columns else 0
    checks.append(("unique_iso3", dup_iso3 == 0, int(dup_iso3)))

    missing_rate = final_df.isna().mean().sort_values(ascending=False)
    top_missing = missing_rate.head(10).reset_index()
    top_missing.columns = ["field", "missing_rate"]
    checks.append(("missingness_report_generated", True, "see qa_missingness_report.csv"))

    for col in ["population", "gdp_usd", "gdp_per_capita_usd", "life_expectancy"]:
        if col in final_df.columns:
            bad = (final_df[col].dropna() < 0).sum()
            checks.append((f"{col}_non_negative", bad == 0, int(bad)))

    report = pd.DataFrame(checks, columns=["check", "passed", "details"])
    return bool(report["passed"].all()), top_missing


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(root, "data", "raw")
    processed_dir = os.path.join(root, "data", "processed")
    ensure_dirs(raw_dir, processed_dir)

    print("1) Fetching country master data…")
    countries = fetch_countries_master()
    if countries.empty:
        raise RuntimeError("Country master dataset returned zero rows.")

    countries.to_csv(os.path.join(raw_dir, "countries_master.csv"), index=False)

    print("2) Pulling indicators (best-effort) from World Bank…")
    start_year, end_year = 2010, datetime.now(timezone.utc).year
    indicator_latest_frames = []

    for name, code in INDICATORS.items():
        try:
            print(f"   - {name} ({code})")
            ind = fetch_indicator_bulk(code, start_year, end_year)
            ind.to_csv(os.path.join(raw_dir, f"indicator_{name}.csv"), index=False)
            indicator_latest_frames.append(latest_by_country(ind, name))
        except Exception:
            indicator_latest_frames.append(pd.DataFrame(columns=["iso3", name, f"{name}_year"]))

    print("3) Merging datasets…")
    final_df = countries.copy()

    for frame in indicator_latest_frames:
        if "iso3" in frame.columns and frame.shape[0] > 0:
            final_df = final_df.merge(frame, on="iso3", how="left")

    as_of = datetime.now(timezone.utc).isoformat(timespec="seconds")
    final_df["as_of_utc"] = as_of

    ok, missing_report = run_quality_checks(final_df)

    final_df.to_csv(os.path.join(processed_dir, "country_insights_latest.csv"), index=False)
    final_df.to_parquet(os.path.join(processed_dir, "country_insights_latest.parquet"), index=False)

    missing_report.to_csv(os.path.join(processed_dir, "qa_missingness_report.csv"), index=False)
    with open(os.path.join(processed_dir, "qa_status.json"), "w", encoding="utf-8") as f:
        json.dump({"passed": ok, "as_of_utc": as_of, "rows": int(final_df.shape[0])}, f, indent=2)

    print(f"Done ✅ QA passed: {ok}")
    print("Saved: data/processed/country_insights_latest.csv")


if __name__ == "__main__":
    main()
