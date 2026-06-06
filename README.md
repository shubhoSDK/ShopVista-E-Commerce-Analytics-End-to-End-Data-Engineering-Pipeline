# ShopVista E-Commerce Analytics — End-to-End Data Engineering Pipeline

> **Azure Databricks · Delta Lake · PySpark · Unity Catalog · Power BI**  
> Medallion architecture (Bronze → Silver → Gold) for a global e-commerce platform serving 7 countries.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Pipeline Structure](#pipeline-structure)
  - [Setup](#setup)
  - [Dimension Tables](#dimension-tables)
  - [Fact Tables — Daily Orders](#fact-tables--daily-orders)
  - [Fact Tables — Monthly Returns & Shipments](#fact-tables--monthly-returns--shipments)
- [Gold Layer Schema](#gold-layer-schema)
- [Processed Data Files](#processed-data-files)
- [Dashboard](#dashboard)
- [Key Numbers](#key-numbers)
- [Tech Stack](#tech-stack)
- [Repo Structure](#repo-structure)
- [How to Run](#how-to-run)

---

## Project Overview

This project builds a **production-grade lakehouse pipeline** for ShopVista, processing:

| Dataset | Scope | Volume |
|---------|-------|--------|
| Daily order items | Jan 2024 – Dec 2025 | ~1.9M rows across 730 files |
| Monthly returns | Jan 2024 – Jan 2026 | 24,604 records across 25 files |
| Monthly shipments | Jan 2024 – Dec 2025 | 812,705 records across 24 files |
| Dimension tables | Customers, Products, Brands, Categories, Date | 300K+ customers · 50K products |

Raw CSV files land in **Azure Data Lake Storage Gen2**. Azure Databricks processes them through three medallion layers into analytics-ready Delta tables, governed by **Unity Catalog**. Results are visualised in **Power BI**.

---

## Architecture

```
ShopVista System
      │
      │  CSV files
      ▼
Azure Data Lake Storage Gen2 (ecomm-raw-data container)
      │
      │  Access Connector (secure managed identity)
      ▼
┌─────────────────────────────────────────────────┐
│              Azure Databricks                   │
│                                                 │
│   Unity Catalog  ──  ecommerce catalog          │
│                                                 │
│   Bronze ──► Silver ──► Gold                    │
│   (raw)    (clean)   (enriched)                 │
└─────────────────────────────────────────────────┘
      │
      │  Delta tables (Gold layer)
      ▼
Power BI Dashboard
```

**Incremental ingestion strategy:**

- **Autoloader** (`cloudFiles`) picks up new files automatically — no re-processing, no missed files.
- **Checkpoints** track exactly which files have been processed per stream.
- **Delta MERGE** (`foreachBatch`) makes every upsert idempotent — safe to rerun on failure.
- **Change Data Feed** lets the gold layer read only what silver changed, avoiding full table scans.
- **`trigger(availableNow=True)`** gives batch simplicity with streaming code structure.

---

## Data Sources

### Historical full load (Jan 2024 – Aug 2025)

```
ecomm-raw-data/
└── historical-full-load/
    ├── brands/brands.csv
    ├── category/category.csv
    ├── customers/customers.csv
    ├── date/date.csv
    ├── products/products.csv
    ├── order_items/landing/order_items_YYYY-MM-DD.csv   (one per day)
    ├── order_returns/landing/returns_YYYY-MM.csv        (one per month)
    └── order_shipments/landing/shipments_YYYY-MM.csv    (one per month)
```

### Incremental load (Sep 2025 onwards)

```
ecomm-raw-data/
└── incremental-load/
    ├── order_items/incoming/order_items_YYYY-MM-DD.csv
    ├── order_returns/incoming/returns_YYYY-MM.csv
    └── order_shipments/incoming/shipments_YYYY-MM.csv
```

---

## Pipeline Structure

### Setup

| Notebook | Purpose |
|----------|---------|
| `1_setup/setup_catalog.ipynb` | Creates the `ecommerce` catalog with Bronze, Silver, Gold schemas in Unity Catalog |
| `1_setup/setup_raw_external_volume.ipynb` | Mounts ADLS Gen2 container as an External Volume (`raw.raw_landing`) |

### Dimension Tables

| Notebook | Bronze Table | Silver Table | Gold Table |
|----------|-------------|-------------|------------|
| `2_medallion_processing_dim/1_dim_bronze.ipynb` | `brz_brands` `brz_category` `brz_products` `brz_customers` `brz_calendar` | — | — |
| `2_medallion_processing_dim/2_dim_silver.ipynb` | — | `slv_brands` `slv_category` `slv_products` `slv_customers` `slv_calendar` | — |
| `2_medallion_processing_dim/3_dim_gold.ipynb` | — | — | `gld_dim_products` `gld_dim_customers` `gld_dim_date` |

**Key cleaning applied in silver:**

- Brands: trim whitespace, strip special chars from `brand_code`, normalise `category_code` anomalies (`GROCERY`→`GRCY`, `BOOKS`→`BKS`, `TOYS`→`TOY`)
- Products: strip `g` suffix from `weight_grams`, replace `,` decimal separator in `length_cm`, fix material typos (`Coton`→`Cotton`, `Ruber`→`Rubber`, `Alumium`→`Aluminum`), set negative `rating_count` to absolute value
- Customers: drop rows where `customer_id` is null, fill null `phone` with `'Not Available'`
- Calendar: deduplicate on `date`, normalise `day_name` casing, convert negative `week_of_year` to positive, format `quarter` as `Q{n}-YYYY` and `week` as `Week{n}-YYYY`

### Fact Tables — Daily Orders

| Notebook | Layer | Table | Notes |
|----------|-------|-------|-------|
| `3_medallion_processing_fact/1_fact_bronze.ipynb` | Bronze | `brz_order_items` | Autoloader, append-only, schema rescue |
| `3_medallion_processing_fact/2_fact_silver.ipynb` | Silver | `slv_order_items` | Dedup on `(order_id, item_seq)`, clean quantity/price/discount/channel, Delta MERGE upsert, CDF enabled |
| `3_medallion_processing_fact/3_fact_gold.ipynb` | Gold | `gld_fact_order_items` | CDF-driven, adds `gross_amount`, `discount_amount`, `net_amount`, `date_id`, `coupon_flag`, Delta MERGE |
| `3_medallion_processing_fact/4_daily_summary.ipynb` | Gold | `gld_fact_daily_orders_summary` | Rolling 30-day MERGE by `(date_id, currency)` |

**Data quality issues handled in silver:**

```python
# Text quantity  →  integer
"Two" → 2

# Currency symbol in price
"$27" → 27.0

# Percentage in discount
"16%" → 16.0

# Channel normalisation
"web" → "Website"
"app" → "Mobile"

# Coupon code standardisation
"NEW10" → "new10"  (lowercase + trim)
```

### Fact Tables — Monthly Returns & Shipments

> This is the **capstone exercise** — three new notebooks implementing the full medallion pipeline for monthly operational data.

| Notebook | Layer | Tables |
|----------|-------|--------|
| `5_exercise/1_monthly_data_bronze_processing.py` | Bronze | `brz_returns` · `brz_shipments` |
| `5_exercise/2_monthly_data_silver_processing.py` | Silver | `slv_returns` · `slv_shipments` |
| `5_exercise/3_monthly_data_gold_processing.py` | Gold | `gld_fact_order_returns` · `gld_fact_order_shipments` |

**Returns — silver transformations:**

- Deduplicate on `(order_id, order_dt, return_ts)`
- Cast `order_dt` → `DateType`
- Cast `return_ts` → `TimestampType`
- `reason` → UPPERCASE + trim
- Add `processed_time`

**Returns — gold enrichment:**

| Column | Logic |
|--------|-------|
| `date_id` | `order_dt` as `yyyyMMdd` integer (FK to `gld_dim_date`) |
| `return_days` | Days between `return_ts` and `order_dt` |
| `within_policy` | `1` if `return_days` ≤ 15, else `0` |
| `is_late_return` | `1` if `return_days` > 15, else `0` |

**Shipments — silver transformations:**

- Cast `order_dt` → `DateType`
- `carrier` → title-case + trim
- Add `processed_time`

**Shipments — gold enrichment:**

| Column | Logic |
|--------|-------|
| `carrier_group` | `Domestic` if carrier is EcomExpress / Delhivery / XpressBees / BlueDart; else `International` |
| `is_weekend_shipment` | `True` if `order_dt` falls on Saturday or Sunday |

**Orchestration — `monthly_job`:**

```
monthly_bronze_processing  →  monthly_silver_processing  →  monthly_gold_processing
```

Scheduled: **1st of every month at 02:00 AM** (cron: `0 2 1 * *`)  
Each notebook accepts `catalog_name`, `storage_account_name`, `container_name` as job parameters.

---

## Gold Layer Schema

### `gld_dim_products`

| Column | Type | Notes |
|--------|------|-------|
| `product_id` | string | PK |
| `sku` | string | |
| `category_code` | string | FK → `gld_dim_date` |
| `category_name` | string | Enriched from category dim |
| `brand_code` | string | |
| `brand_name` | string | Enriched from brands dim |
| `color` · `size` · `material` | string | Cleaned |
| `weight_grams` · `length_cm` · `width_cm` · `height_cm` | numeric | Cleaned |
| `rating_count` | integer | Absolute value applied |

### `gld_dim_customers`

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | string | PK |
| `phone` | string | Null filled with 'Not Available' |
| `country_code` · `country` · `state` | string | |
| `region` | string | Mapped from country+state (e.g. MH → West, TN → South) |

### `gld_dim_date`

| Column | Type | Notes |
|--------|------|-------|
| `date_id` | integer | PK — yyyyMMdd |
| `date` | date | |
| `year` · `month_name` · `day_name` | string/int | |
| `is_weekend` | integer | 1 = Sat/Sun |
| `quarter` | string | e.g. `Q1-2024` |
| `week` | string | e.g. `Week1-2024` |

### `gld_fact_order_items`

| Column | Type | Notes |
|--------|------|-------|
| `date_id` | integer | FK → `gld_dim_date` |
| `transaction_date` · `transaction_ts` | date/timestamp | |
| `transaction_id` | integer | Order ID |
| `customer_id` | string | FK → `gld_dim_customers` |
| `product_id` | string | FK → `gld_dim_products` |
| `channel` · `coupon_code` · `coupon_flag` | string/int | |
| `unit_price_currency` · `quantity` · `unit_price` | various | Cleaned |
| `gross_amount` | double | `quantity × unit_price` |
| `discount_percent` · `discount_amount` | double/long | |
| `tax_amount` | integer | |
| `net_amount` | double | `gross − discount + tax` |

### `gld_fact_order_returns` *(new)*

| Column | Type | Notes |
|--------|------|-------|
| `date_id` | integer | FK → `gld_dim_date` |
| `order_dt` | date | |
| `return_ts` | timestamp | |
| `order_id` | integer | FK → `gld_fact_order_items` |
| `reason` | string | Uppercased |
| `return_days` | integer | Days from order to return |
| `within_policy` | integer | 1 = ≤ 15 days |
| `is_late_return` | integer | 1 = > 15 days |
| `processed_time` | timestamp | |

### `gld_fact_order_shipments` *(new)*

| Column | Type | Notes |
|--------|------|-------|
| `order_dt` | date | |
| `shipment_id` | string | PK |
| `order_id` | integer | FK → `gld_fact_order_items` |
| `carrier` | string | Title-cased |
| `carrier_group` | string | `Domestic` or `International` |
| `is_weekend_shipment` | boolean | True = Sat/Sun dispatch |
| `processed_time` | timestamp | |

### `gld_fact_daily_orders_summary`

Aggregated daily roll-up by `(date_id, currency)` — updated via rolling 30-day MERGE.

| Column | Type |
|--------|------|
| `date_id` · `currency` | string/int |
| `total_quantity` · `total_gross_amount` | aggregated |
| `total_discount_amount` · `total_tax_amount` · `total_amount` | aggregated |

---

## Processed Data Files

Pre-computed gold layer outputs from the complete dataset (Jan 2024 – Jan 2026) are provided in `processed_data/`:

```
processed_data/
├── silver/
│   ├── returns/slv_returns.csv          (24,604 rows)
│   └── shipments/slv_shipments.csv      (812,705 rows)
└── gold/
    ├── returns/gld_fact_order_returns.csv     (24,604 rows)
    └── shipments/gld_fact_order_shipments.csv (812,705 rows)
```

These are the exact outputs your Databricks pipeline would produce in the Unity Catalog gold schema.

---

## Dashboard

The Power BI report (`4_dashboarding/ecommerce_analytics.pbix`) connects to the gold layer and contains two pages:

**Page 1 — E-com View 1**

- KPI cards: Total Sales, Units Sold, Repeat Customer Rate, Total Customers, Avg Discount %, High-Profit Region
- Brand × Category net revenue table
- Customer count by region (horizontal bar chart)
- Total sales by channel (donut)
- Revenue trend by month (line chart)
- Sales by country (bar chart)
- Slicers: Country, Brand, Category, Date range

**Page 2 — Monthly Returns & Shipments** *(new)*

- KPI cards: Total Returns, Within Policy, Late Returns, Avg Return Days, Total Shipments, Weekend Dispatch %
- Monthly return volume (stacked bar — within policy vs late)
- Returns by reason (horizontal bar)
- Policy compliance scorecard
- Monthly shipment volume (stacked bar — domestic vs international)
- Carrier split donut + carrier volume bars
- Slicers: Year, Policy filter, Carrier group

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Net revenue (INR) | ₹14.02bn |
| Units sold | 1.90M |
| Unique customers | 299,700 |
| Repeat customer rate | 80.9% |
| Avg discount | 8.67% |
| Top region | South India — ₹5.56bn |
| Top category | Electronics — ₹10.64bn |
| Total returns | 24,604 |
| Within-policy returns | 93.4% (22,975) |
| Avg return days | 11.5 |
| Total shipments | 812,705 |
| Domestic shipments | 49.9% (405,821) |
| Weekend shipments | 33.0% |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (PySpark) |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Ingestion | Databricks Autoloader (cloudFiles) |
| Streaming | Structured Streaming — `foreachBatch`, CDF, `trigger(availableNow)` |
| Orchestration | Databricks Jobs (`monthly_job`) |
| BI layer | Power BI |
| Languages | Python, PySpark SQL |

---

## Repo Structure

```
spark_project_ecommerce/
│
├── 0_data/
│   └── ecomm-raw-data.zip              # Raw source data (ADLS Gen2 mirror)
│
├── 1_setup/
│   ├── setup_catalog.ipynb             # Create catalog + schemas
│   └── setup_raw_external_volume.ipynb # Mount ADLS as External Volume
│
├── 2_medallion_processing_dim/
│   ├── 1_dim_bronze.ipynb              # Ingest dimension CSVs → Bronze
│   ├── 2_dim_silver.ipynb              # Clean + deduplicate → Silver
│   └── 3_dim_gold.ipynb                # Enrich + join → Gold
│
├── 3_medallion_processing_fact/
│   ├── 1_fact_bronze.ipynb             # Autoloader daily order items → Bronze
│   ├── 2_fact_silver.ipynb             # Clean + MERGE → Silver (CDF enabled)
│   ├── 3_fact_gold.ipynb               # CDF-driven enrichment → Gold
│   └── 4_daily_summary.ipynb           # Rolling 30-day summary MERGE
│
├── 4_dashboarding/
│   └── ecommerce_analytics.pbix        # Power BI report (2 pages)
│
├── 5_exercise/                         # Monthly ETL capstone
│   ├── 1_monthly_data_bronze_processing.py
│   ├── 2_monthly_data_silver_processing.py
│   ├── 3_monthly_data_gold_processing.py
│   └── monthly_etl_assignment_instructions.pdf
│
├── processed_data/                     # Pre-computed gold outputs (CSV)
│   ├── silver/returns/slv_returns.csv
│   ├── silver/shipments/slv_shipments.csv
│   ├── gold/returns/gld_fact_order_returns.csv
│   └── gold/shipments/gld_fact_order_shipments.csv
│
└── resources/
    ├── project_architecture.png
    ├── project_architecture.svg
    └── ecommerce_analytics_report.jpg
```

---

## How to Run

### Prerequisites

- Azure Databricks workspace (Runtime 13.3 LTS or above)
- Azure Data Lake Storage Gen2 account with the `ecomm-raw-data` container
- Azure Access Connector linked to the Databricks workspace
- Unity Catalog metastore configured on the workspace

### Step 1 — Upload raw data

Upload the contents of `0_data/ecomm-raw-data.zip` to your ADLS Gen2 container, preserving the folder structure (`historical-full-load/`, `incremental-load/`).

### Step 2 — Run setup notebooks

```
1_setup/setup_catalog.ipynb            # creates ecommerce catalog + schemas
1_setup/setup_raw_external_volume.ipynb  # creates raw.raw_landing volume
```

### Step 3 — Run dimension pipeline

```
2_medallion_processing_dim/1_dim_bronze.ipynb
2_medallion_processing_dim/2_dim_silver.ipynb
2_medallion_processing_dim/3_dim_gold.ipynb
```

### Step 4 — Run fact pipeline (daily orders)

```
3_medallion_processing_fact/1_fact_bronze.ipynb
3_medallion_processing_fact/2_fact_silver.ipynb
3_medallion_processing_fact/3_fact_gold.ipynb
3_medallion_processing_fact/4_daily_summary.ipynb
```

### Step 5 — Run monthly ETL pipeline

```
5_exercise/1_monthly_data_bronze_processing.py
5_exercise/2_monthly_data_silver_processing.py
5_exercise/3_monthly_data_gold_processing.py
```

Each notebook accepts these widgets / job parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog_name` | `ecommerce` | Unity Catalog name |
| `storage_account_name` | `ecommcistorage05` | ADLS Gen2 account name |
| `container_name` | `ecomm-raw-data` | ADLS container name |

### Step 6 — Schedule with Databricks Jobs

Create a job named `monthly_job` with three tasks in sequence:

```
monthly_bronze_processing → monthly_silver_processing → monthly_gold_processing
```

Schedule: `0 2 1 * *` (1st of every month at 02:00 AM)

### Step 7 — Connect Power BI

Open `4_dashboarding/ecommerce_analytics.pbix` in Power BI Desktop and update the Databricks SQL warehouse connection to point to your workspace. The report connects directly to the gold schema in Unity Catalog.

---

*Built with Azure Databricks · Delta Lake · PySpark · Unity Catalog · Power BI*
