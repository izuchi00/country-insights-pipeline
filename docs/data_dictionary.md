# Country Insights — Data Dictionary

This document describes the fields in:
- `data/processed/country_insights_latest.csv`
- `data/processed/country_insights_latest.parquet`

All timestamps are UTC.

## Keys

**iso3**  
Type: string (3 chars)  
Description: ISO 3166-1 alpha-3 country code (primary key).  
Example: GBR

## Country metadata

**country**  
Type: string  
Description: Country name from the country master dataset.  
Example: United Kingdom

**region**  
Type: string  
Description: High-level geographic region.  
Example: Europe

**subregion**  
Type: string  
Description: Subregion classification under `region`.  
Example: Northern Europe

**capital**  
Type: string  
Description: Capital city name from the country master dataset.  
Example: London

**lat**  
Type: number (float)  
Description: Latitude associated with the country/capital reference point.  
Example: 54.0

**lon**  
Type: number (float)  
Description: Longitude associated with the country/capital reference point.  
Example: -2.0

## Population fields (current output)

**population_x**  
Type: integer  
Description: Population value from the country master dataset (source-dependent).  
Notes: Present for territories that may not exist in World Bank indicators.

**population_y**  
Type: number (float)  
Description: Population value sourced from the World Bank indicator pull.  
Notes: Expected to align with `population_year` when present.

**population_year**  
Type: integer  
Description: Year associated with `population_y` (latest available year pulled).  
Example: 2024

## World Bank indicators (latest available)

**gdp_usd**  
Type: number (float)  
Description: GDP, current US$ (latest available year).  
Indicator: NY.GDP.MKTP.CD

**gdp_usd_year**  
Type: integer  
Description: Year associated with `gdp_usd`.

**gdp_per_capita_usd**  
Type: number (float)  
Description: GDP per capita, current US$ (latest available year).  
Indicator: NY.GDP.PCAP.CD

**gdp_per_capita_usd_year**  
Type: integer  
Description: Year associated with `gdp_per_capita_usd`.

**life_expectancy**  
Type: number (float)  
Description: Life expectancy at birth, total (years; latest available year).  
Indicator: SP.DYN.LE00.IN

**life_expectancy_year**  
Type: integer  
Description: Year associated with `life_expectancy`.

## Pipeline metadata

**as_of_utc**  
Type: string (ISO 8601 timestamp)  
Description: Pipeline run timestamp in UTC indicating when the dataset was produced.  
Example: 2025-12-15T18:33:17+00:00
