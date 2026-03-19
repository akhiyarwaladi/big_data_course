# ==============================================================================
# PERTEMUAN 10 — Google BigQuery: SQL at Scale
# Studi Kasus: NYC Taxi (miliar baris) + GitHub Repos
# Platform: Google Colab + BigQuery Free Tier
#
# SETUP: Pastikan sudah punya akun Google Cloud (gratis, tanpa kartu kredit)
# ==============================================================================

# %% [1] Setup BigQuery di Colab
from google.colab import auth
auth.authenticate_user()

PROJECT_ID = 'your-project-id'  # <-- GANTI dengan project ID Anda!

!pip install google-cloud-bigquery db-dtypes -q

from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import time

client = bigquery.Client(project=PROJECT_ID)

def bq(sql, max_gb=2):
    """Helper: jalankan BigQuery query dengan safety limit."""
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=int(max_gb * 1e9))
    return client.query(sql, job_config=cfg).to_dataframe()

print(f"✅ Connected: {PROJECT_ID}")

# %% [2] Eksplorasi Public Datasets
# BigQuery punya 200+ dataset publik GRATIS — dari taxi trips hingga genome!

datasets = {
    'NYC Taxi Trips':      'bigquery-public-data.new_york_taxi_trips',
    'GitHub Repos':        'bigquery-public-data.github_repos',
    'Stack Overflow':      'bigquery-public-data.stackoverflow',
    'Google Analytics':    'bigquery-public-data.google_analytics_sample',
    'COVID-19':            'bigquery-public-data.covid19_open_data',
    'Wikipedia':           'bigquery-public-data.wikipedia',
}
for name, ds in datasets.items():
    print(f"  📁 {name:25s} → {ds}")

# %% [3] NYC Taxi — Seberapa besar datanya?
t0 = time.time()
df_meta = bq("""
    SELECT
        COUNT(*)               AS total_trips,
        MIN(pickup_datetime)   AS earliest,
        MAX(pickup_datetime)   AS latest
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
""")
print(f"⚡ Query selesai dalam {time.time()-t0:.1f}s")
print(f"Total trips : {df_meta['total_trips'][0]:,}")
print(f"Range       : {df_meta['earliest'][0]} — {df_meta['latest'][0]}")

# %% [4] ANALISIS 1: Jam Sibuk NYC Taxi
df_hour = bq("""
    SELECT
        EXTRACT(HOUR FROM pickup_datetime)  AS jam,
        COUNT(*)                            AS trips,
        ROUND(AVG(trip_distance), 2)        AS avg_miles,
        ROUND(AVG(total_amount), 2)         AS avg_fare,
        ROUND(AVG(tip_amount), 2)           AS avg_tip,
        ROUND(AVG(tip_amount / NULLIF(total_amount, 0)) * 100, 1) AS tip_pct
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
    WHERE trip_distance > 0 AND total_amount BETWEEN 1 AND 500
    GROUP BY jam
    ORDER BY jam
""", max_gb=3)

print("🚖 NYC Taxi Hourly Pattern 2022:")
print(df_hour.to_string(index=False))

# Visualisasi
fig, ax = plt.subplots(1, 2, figsize=(14, 5))
ax[0].bar(df_hour['jam'], df_hour['trips'], color='#4285F4')
ax[0].set_xlabel('Hour'); ax[0].set_ylabel('Trips'); ax[0].set_title('Trip Volume')
ax[0].set_xticks(range(0, 24))

ax[1].plot(df_hour['jam'], df_hour['avg_fare'], 'o-', color='#34A853', label='Avg Fare')
ax2 = ax[1].twinx()
ax2.plot(df_hour['jam'], df_hour['tip_pct'], 's--', color='#EA4335', alpha=.7, label='Tip %')
ax[1].set_xlabel('Hour'); ax[1].set_ylabel('Fare ($)'); ax2.set_ylabel('Tip %')
ax[1].set_title('Fare & Tipping'); ax[1].legend(loc='upper left'); ax2.legend(loc='upper right')
plt.tight_layout(); plt.show()

# %% [5] ANALISIS 2: Trend Bulanan + YoY
df_month = bq("""
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', pickup_datetime) AS bulan,
        COUNT(*)                                   AS trips,
        ROUND(SUM(total_amount) / 1e6, 2)          AS revenue_M,
        ROUND(AVG(total_amount), 2)                AS avg_fare
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
    WHERE trip_distance > 0 AND total_amount BETWEEN 1 AND 500
    GROUP BY bulan ORDER BY bulan
""", max_gb=3)

print("📈 Monthly Trend:")
print(df_month.to_string(index=False))

fig, ax1 = plt.subplots(figsize=(12, 5))
ax1.bar(df_month['bulan'], df_month['trips'], color='#4285F4', alpha=.7)
ax2 = ax1.twinx()
ax2.plot(df_month['bulan'], df_month['revenue_M'], 'o-', color='#EA4335', lw=2)
ax1.set_ylabel('Trips'); ax2.set_ylabel('Revenue ($M)')
plt.title('NYC Taxi Monthly: Trips vs Revenue 2022')
plt.xticks(rotation=45); plt.tight_layout(); plt.show()

# %% [6] ANALISIS 3: Top Pickup Zones
df_zones = bq("""
    WITH stats AS (
        SELECT
            pickup_location_id          AS zone,
            COUNT(*)                    AS trips,
            ROUND(AVG(total_amount), 2) AS avg_fare,
            ROUND(SUM(total_amount)/1e6, 2) AS rev_M
        FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
        WHERE trip_distance > 0 AND total_amount BETWEEN 1 AND 500
        GROUP BY zone
    )
    SELECT *, RANK() OVER (ORDER BY trips DESC) AS rank
    FROM stats
    ORDER BY trips DESC
    LIMIT 15
""", max_gb=3)

print("🗽 Top 15 Pickup Zones:")
print(df_zones.to_string(index=False))

# %% [7] Advanced SQL: CTE + Window Functions
df_daily = bq("""
    WITH daily AS (
        SELECT
            DATE(pickup_datetime) AS dt,
            COUNT(*)              AS trips,
            SUM(total_amount)     AS rev
        FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
        WHERE total_amount BETWEEN 1 AND 500
          AND pickup_datetime >= '2022-06-01'
          AND pickup_datetime <  '2022-07-01'
        GROUP BY dt
    ),
    enriched AS (
        SELECT
            dt,
            trips,
            ROUND(rev / 1e3, 1)  AS rev_K,
            ROUND(AVG(rev/1e3) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS ma7,
            ROUND((rev - LAG(rev) OVER (ORDER BY dt)) / NULLIF(LAG(rev) OVER (ORDER BY dt), 0) * 100, 1) AS pct_chg,
            FORMAT_DATE('%A', dt) AS day_name
        FROM daily
    )
    SELECT * FROM enriched ORDER BY dt
""", max_gb=3)

print("📊 June 2022 Daily with Moving Average & % Change:")
print(df_daily.to_string(index=False))

# %% [8] Advanced SQL: APPROX functions — untuk data sangat besar
df_approx = bq("""
    SELECT
        EXTRACT(MONTH FROM pickup_datetime) AS mo,
        COUNT(DISTINCT pickup_location_id)       AS exact_zones,
        APPROX_COUNT_DISTINCT(pickup_location_id) AS approx_zones,
        APPROX_QUANTILES(total_amount, 4)[OFFSET(2)] AS median_fare
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
    WHERE total_amount BETWEEN 1 AND 500
    GROUP BY mo ORDER BY mo
""", max_gb=3)

print("⚡ Approximate Functions (super cepat untuk data besar):")
print(df_approx.to_string(index=False))
print("\nExact vs Approx: hampir identik, tapi approx jauh lebih cepat untuk TB-scale!")

# %% [9] GitHub Dataset — Bahasa Pemrograman Terpopuler
df_lang = bq("""
    SELECT
        l.name              AS bahasa,
        COUNT(DISTINCT repo_name) AS repos
    FROM `bigquery-public-data.github_repos.languages` t,
         UNNEST(language) l
    GROUP BY bahasa
    ORDER BY repos DESC
    LIMIT 20
""", max_gb=5)

print("💻 Top 20 Programming Languages on GitHub:")
print(df_lang.to_string(index=False))

plt.figure(figsize=(10, 7))
plt.barh(df_lang['bahasa'][::-1], df_lang['repos'][::-1], color='#4285F4')
plt.xlabel('Repositories'); plt.title('Most Popular Languages on GitHub')
plt.tight_layout(); plt.show()

# %% [10] GitHub — Kapan developer paling aktif?
df_commits = bq("""
    SELECT
        EXTRACT(DAYOFWEEK FROM author.date) AS dow,
        CASE EXTRACT(DAYOFWEEK FROM author.date)
            WHEN 1 THEN 'Minggu' WHEN 2 THEN 'Senin' WHEN 3 THEN 'Selasa'
            WHEN 4 THEN 'Rabu'   WHEN 5 THEN 'Kamis' WHEN 6 THEN 'Jumat'
            WHEN 7 THEN 'Sabtu'
        END AS hari,
        COUNT(*)                     AS commits,
        COUNT(DISTINCT committer.name) AS devs
    FROM `bigquery-public-data.github_repos.commits`
    WHERE author.date BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2024-01-01')
    GROUP BY dow, hari
    ORDER BY dow
""", max_gb=5)

print("📅 Developer Productivity per Day (2023):")
print(df_commits.to_string(index=False))

# %% [11] Pattern: BigQuery → DuckDB (cloud to local)
!pip install duckdb -q
import duckdb

# Query besar di BigQuery, analisis detail di DuckDB (lokal)
df_agg = bq("""
    SELECT
        DATE(pickup_datetime)       AS dt,
        EXTRACT(HOUR FROM pickup_datetime) AS hr,
        pickup_location_id          AS zone,
        COUNT(*)                    AS trips,
        ROUND(AVG(total_amount), 2) AS avg_fare,
        ROUND(AVG(tip_amount / NULLIF(total_amount,0) * 100), 1) AS tip_pct
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
    WHERE total_amount BETWEEN 1 AND 500 AND trip_distance > 0
    GROUP BY dt, hr, zone
""", max_gb=5)

print(f"📦 Downloaded {len(df_agg):,} aggregated rows from BigQuery\n")

local = duckdb.connect()
local.register('taxi', df_agg)

print("🔍 Analisis lanjutan di DuckDB (lokal, instan):")
local.sql("""
    SELECT zone, SUM(trips) AS total, ROUND(AVG(avg_fare), 2) AS avg_fare,
           ROUND(AVG(tip_pct), 1) AS avg_tip_pct
    FROM taxi GROUP BY zone ORDER BY total DESC LIMIT 10
""").show()

local.close()
print("✅ Praktikum selesai!")
