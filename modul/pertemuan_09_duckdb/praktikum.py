# ==============================================================================
# PERTEMUAN 9 — DuckDB: Database Analitik Modern
# Studi Kasus: Analisis 10 Juta Transaksi GoRide
# Platform: Google Colab
#
# Cara pakai: Copy setiap cell (ditandai # %%) ke cell baru di Google Colab
# ==============================================================================

# %% [1] Instalasi & Setup
!pip install duckdb polars pyarrow -q

import duckdb
import time
import os

con = duckdb.connect()
print(f"DuckDB v{duckdb.__version__} ready!")

# %% [2] Hello DuckDB — Query pertama
# DuckDB bisa langsung query tanpa buat tabel!

con.sql("""
    SELECT * FROM (VALUES
        ('Ahmad', 'Jakarta', 25000),
        ('Budi',  'Bandung', 18000),
        ('Citra', 'Surabaya', 22000),
        ('Diana', 'Jakarta', 30000),
        ('Eko',   'Bandung', 15000)
    ) AS ojek(nama_driver, kota, pendapatan)
""").show()

# Langsung aggregate!
con.sql("""
    SELECT
        kota,
        COUNT(*)            AS jumlah_driver,
        ROUND(AVG(pendapatan))  AS rata_rata,
        SUM(pendapatan)     AS total
    FROM (VALUES
        ('Ahmad','Jakarta',25000), ('Budi','Bandung',18000),
        ('Citra','Surabaya',22000), ('Diana','Jakarta',30000),
        ('Eko','Bandung',15000)
    ) AS ojek(nama_driver, kota, pendapatan)
    GROUP BY kota
    ORDER BY total DESC
""").show()

# %% [3] Generate Dataset — 10 Juta Trip GoRide
NUM_TRIPS = 10_000_000

print(f"Generating {NUM_TRIPS:,} trip records...")
start = time.time()

con.sql(f"""
CREATE OR REPLACE TABLE trips AS
WITH rng AS (
    SELECT
        row_number() OVER () AS rn,
        random() AS r_waktu1, random() AS r_waktu2, random() AS r_waktu3,
        random() AS r_kota, random() AS r_layanan, random() AS r_jarak,
        random() AS r_jarak_val, random() AS r_durasi,
        random() AS r_tarif, random() AS r_tarif_val,
        random() AS r_rating, random() AS r_bayar,
        random() AS r_promo, random() AS r_status
    FROM generate_series(1, {NUM_TRIPS})
)
SELECT
    'TRIP-' || LPAD(CAST(rn AS VARCHAR), 9, '0') AS trip_id,

    TIMESTAMP '2025-01-01'
        + INTERVAL (r_waktu1 * 90) DAY
        + INTERVAL (r_waktu2 * 24) HOUR
        + INTERVAL (r_waktu3 * 60) MINUTE
    AS waktu_pesan,

    CASE
        WHEN r_kota < 0.28 THEN 'Jakarta'
        WHEN r_kota < 0.43 THEN 'Surabaya'
        WHEN r_kota < 0.55 THEN 'Bandung'
        WHEN r_kota < 0.64 THEN 'Medan'
        WHEN r_kota < 0.72 THEN 'Semarang'
        WHEN r_kota < 0.79 THEN 'Makassar'
        WHEN r_kota < 0.85 THEN 'Yogyakarta'
        WHEN r_kota < 0.90 THEN 'Palembang'
        WHEN r_kota < 0.95 THEN 'Denpasar'
        ELSE 'Malang'
    END AS kota,

    CASE
        WHEN r_layanan < 0.35 THEN 'GoRide'
        WHEN r_layanan < 0.58 THEN 'GoCar'
        WHEN r_layanan < 0.78 THEN 'GoFood'
        WHEN r_layanan < 0.90 THEN 'GoSend'
        ELSE 'GoMart'
    END AS layanan,

    ROUND(CASE
        WHEN r_jarak < 0.35 THEN 1 + r_jarak_val * 9
        WHEN r_jarak < 0.58 THEN 2 + r_jarak_val * 18
        ELSE 0.5 + r_jarak_val * 6
    END, 2) AS jarak_km,

    ROUND(3 + r_durasi * 57, 1) AS durasi_menit,

    ROUND(CASE
        WHEN r_tarif < 0.35 THEN 7000 + r_tarif_val * 43000
        WHEN r_tarif < 0.58 THEN 15000 + r_tarif_val * 85000
        WHEN r_tarif < 0.78 THEN 12000 + r_tarif_val * 108000
        ELSE 8000 + r_tarif_val * 32000
    END, -2) AS tarif,

    CASE
        WHEN r_rating < 0.02 THEN 1
        WHEN r_rating < 0.06 THEN 2
        WHEN r_rating < 0.14 THEN 3
        WHEN r_rating < 0.38 THEN 4
        ELSE 5
    END AS rating,

    CASE
        WHEN r_bayar < 0.35 THEN 'GoPay'
        WHEN r_bayar < 0.55 THEN 'OVO'
        WHEN r_bayar < 0.70 THEN 'Dana'
        WHEN r_bayar < 0.85 THEN 'Cash'
        ELSE 'ShopeePay'
    END AS metode_bayar,

    r_promo < 0.25 AS pakai_promo,

    CASE
        WHEN r_status < 0.84 THEN 'completed'
        WHEN r_status < 0.91 THEN 'cancelled_user'
        WHEN r_status < 0.96 THEN 'cancelled_driver'
        ELSE 'no_driver'
    END AS status

FROM rng
""")

elapsed = time.time() - start
count = con.sql("SELECT COUNT(*) FROM trips").fetchone()[0]
print(f"✅ {count:,} rows dalam {elapsed:.1f}s")

# %% [4] Preview data
con.sql("SELECT * FROM trips LIMIT 10").show()
con.sql("""
    SELECT
        COUNT(*)                    AS total_rows,
        COUNT(DISTINCT kota)        AS kota_unik,
        COUNT(DISTINCT layanan)     AS layanan_unik,
        MIN(waktu_pesan)::DATE      AS tanggal_awal,
        MAX(waktu_pesan)::DATE      AS tanggal_akhir
    FROM trips
""").show()

# %% [5] ANALISIS 1: Performa per Kota
con.sql("""
    SELECT
        kota,
        COUNT(*)                                            AS total_trip,
        ROUND(AVG(tarif))                                   AS avg_tarif,
        ROUND(SUM(tarif) / 1e9, 2)                          AS revenue_miliar,
        ROUND(AVG(rating), 2)                               AS avg_rating,
        ROUND(AVG(jarak_km), 1)                             AS avg_jarak,
        ROUND(COUNT(*) FILTER (WHERE status = 'completed')
              * 100.0 / COUNT(*), 1)                        AS completion_pct
    FROM trips
    GROUP BY kota
    ORDER BY revenue_miliar DESC
""").show()

# %% [6] ANALISIS 2: Pola Jam Sibuk
con.sql("""
    SELECT
        EXTRACT(HOUR FROM waktu_pesan)      AS jam,
        COUNT(*)                            AS trips,
        ROUND(AVG(tarif))                   AS avg_tarif,
        REPEAT('█', GREATEST((COUNT(*) / 8000)::INT, 1)) AS volume
    FROM trips
    WHERE status = 'completed'
    GROUP BY jam
    ORDER BY jam
""").show()

# %% [7] ANALISIS 3: Revenue per Layanan
con.sql("""
    SELECT
        layanan,
        COUNT(*)                                AS trips,
        ROUND(AVG(tarif))                       AS avg_tarif,
        ROUND(SUM(tarif) / 1e9, 2)              AS revenue_miliar,
        ROUND(AVG(jarak_km), 1)                 AS avg_km,
        ROUND(AVG(rating), 2)                   AS avg_rating,
        ROUND(COUNT(*) FILTER (WHERE pakai_promo)
              * 100.0 / COUNT(*), 1)            AS promo_pct
    FROM trips
    WHERE status = 'completed'
    GROUP BY layanan
    ORDER BY revenue_miliar DESC
""").show()

# %% [8] ANALISIS 4: Window Functions — Trend Harian
con.sql("""
    WITH harian AS (
        SELECT
            waktu_pesan::DATE               AS tanggal,
            COUNT(*)                        AS trips,
            ROUND(SUM(tarif) / 1e6, 1)     AS revenue_juta
        FROM trips
        WHERE status = 'completed'
        GROUP BY tanggal
    )
    SELECT
        tanggal,
        trips,
        revenue_juta,

        -- Moving average 7 hari
        ROUND(AVG(revenue_juta) OVER (
            ORDER BY tanggal ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1) AS ma7,

        -- Delta dari hari sebelumnya
        ROUND(revenue_juta - LAG(revenue_juta) OVER (ORDER BY tanggal), 1)
            AS delta,

        -- Ranking revenue
        RANK() OVER (ORDER BY revenue_juta DESC) AS rank_rev

    FROM harian
    ORDER BY tanggal
    LIMIT 25
""").show()

# %% [9] ANALISIS 5: Adopsi Pembayaran Digital vs Cash
con.sql("""
    SELECT
        metode_bayar,
        COUNT(*)                                        AS transaksi,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS persen,
        ROUND(SUM(tarif) / 1e9, 2)                     AS rev_miliar,
        REPEAT('█', (COUNT(*) * 40 / SUM(COUNT(*)) OVER())::INT) AS bar
    FROM trips WHERE status = 'completed'
    GROUP BY metode_bayar
    ORDER BY transaksi DESC
""").show()

# Digital vs Cash
con.sql("""
    SELECT
        CASE WHEN metode_bayar = 'Cash' THEN 'Cash' ELSE 'Digital' END AS tipe,
        COUNT(*)                                        AS total,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS persen
    FROM trips WHERE status = 'completed'
    GROUP BY tipe
""").show()

# %% [10] BENCHMARK: DuckDB vs Pandas
import pandas as pd

print("🏎️ BENCHMARK DuckDB vs Pandas")
print(f"Dataset: {NUM_TRIPS:,} rows")
print("=" * 55)

# Load ke Pandas
t0 = time.time()
df = con.sql("SELECT * FROM trips").df()
print(f"Load ke Pandas   : {time.time()-t0:.2f}s\n")

# --- Test 1: Group By ---
t0 = time.time()
df[df['status']=='completed'].groupby('kota').agg(
    total=('trip_id','count'), avg_tarif=('tarif','mean')
).reset_index()
t_pd = time.time() - t0

t0 = time.time()
con.sql("""
    SELECT kota, COUNT(*), AVG(tarif)
    FROM trips WHERE status='completed' GROUP BY kota
""").fetchall()
t_db = time.time() - t0

print(f"Test 1: GROUP BY kota")
print(f"  Pandas : {t_pd:.4f}s")
print(f"  DuckDB : {t_db:.4f}s  → {t_pd/t_db:.0f}x lebih cepat")

# --- Test 2: Filter + Sort ---
t0 = time.time()
df[(df['kota']=='Jakarta') & (df['status']=='completed') & (df['tarif']>50000)] \
    .sort_values('tarif', ascending=False)
t_pd = time.time() - t0

t0 = time.time()
con.sql("""
    SELECT * FROM trips
    WHERE kota='Jakarta' AND status='completed' AND tarif>50000
    ORDER BY tarif DESC
""").fetchall()
t_db = time.time() - t0

print(f"\nTest 2: Filter + Sort")
print(f"  Pandas : {t_pd:.4f}s")
print(f"  DuckDB : {t_db:.4f}s  → {t_pd/t_db:.0f}x lebih cepat")

# --- Test 3: Window Function ---
t0 = time.time()
daily = df[df['status']=='completed'].groupby(df['waktu_pesan'].dt.date)['tarif'].sum()
daily.rolling(7).mean()
t_pd = time.time() - t0

t0 = time.time()
con.sql("""
    WITH d AS (
        SELECT waktu_pesan::DATE AS tgl, SUM(tarif) AS rev
        FROM trips WHERE status='completed' GROUP BY tgl
    )
    SELECT tgl, rev, AVG(rev) OVER (ORDER BY tgl ROWS 6 PRECEDING) AS ma7
    FROM d ORDER BY tgl
""").fetchall()
t_db = time.time() - t0

print(f"\nTest 3: Daily + Moving Average")
print(f"  Pandas : {t_pd:.4f}s")
print(f"  DuckDB : {t_db:.4f}s  → {t_pd/t_db:.0f}x lebih cepat")

del df  # free memory

# %% [11] PARQUET vs CSV — Format Data Modern
# Export kedua format
t0 = time.time()
con.sql("COPY trips TO '/tmp/trips.csv' (FORMAT CSV, HEADER)")
t_csv_w = time.time() - t0
csv_mb = os.path.getsize('/tmp/trips.csv') / 1e6

t0 = time.time()
con.sql("COPY trips TO '/tmp/trips.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
t_pq_w = time.time() - t0
pq_mb = os.path.getsize('/tmp/trips.parquet') / 1e6

print(f"{'Format':<10} {'Size (MB)':<12} {'Write (s)':<12} {'Compression'}")
print(f"{'CSV':<10} {csv_mb:<12.1f} {t_csv_w:<12.2f} {'—'}")
print(f"{'Parquet':<10} {pq_mb:<12.1f} {t_pq_w:<12.2f} {'ZSTD'}")
print(f"\nParquet {csv_mb/pq_mb:.1f}x lebih kecil!")

# Benchmark baca
t0 = time.time()
con.sql("SELECT kota, AVG(tarif) FROM '/tmp/trips.csv' GROUP BY kota").fetchall()
t_csv_r = time.time() - t0

t0 = time.time()
con.sql("SELECT kota, AVG(tarif) FROM '/tmp/trips.parquet' GROUP BY kota").fetchall()
t_pq_r = time.time() - t0

print(f"\nBaca + query:")
print(f"  CSV    : {t_csv_r:.3f}s")
print(f"  Parquet: {t_pq_r:.3f}s  → {t_csv_r/t_pq_r:.0f}x lebih cepat")

# %% [12] Query langsung dari file Parquet (tanpa CREATE TABLE!)
# DuckDB bisa query file langsung — tidak perlu import ke tabel dulu
con.sql("""
    SELECT layanan, COUNT(*) AS trips, ROUND(AVG(tarif)) AS avg
    FROM '/tmp/trips.parquet'
    WHERE status = 'completed'
    GROUP BY layanan
    ORDER BY trips DESC
""").show()

# Bahkan bisa query file remote (URL)!
# con.sql("SELECT * FROM 'https://example.com/data.parquet' LIMIT 10")

# %% [13] Visualisasi dengan Matplotlib
import matplotlib.pyplot as plt

# Data untuk chart
hourly = con.sql("""
    SELECT EXTRACT(HOUR FROM waktu_pesan) AS jam, COUNT(*) AS trips,
           ROUND(AVG(tarif)) AS avg_tarif
    FROM trips WHERE status='completed' GROUP BY jam ORDER BY jam
""").df()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].bar(hourly['jam'], hourly['trips'], color='#4285F4', alpha=0.8)
axes[0].set_xlabel('Jam'); axes[0].set_ylabel('Jumlah Trip')
axes[0].set_title('Volume Trip per Jam')
axes[0].set_xticks(range(0, 24))

axes[1].plot(hourly['jam'], hourly['avg_tarif'], 'o-', color='#34A853', lw=2)
axes[1].set_xlabel('Jam'); axes[1].set_ylabel('Rata-rata Tarif (Rp)')
axes[1].set_title('Rata-rata Tarif per Jam')
axes[1].set_xticks(range(0, 24))

plt.tight_layout()
plt.show()

# Chart per kota
kota_rev = con.sql("""
    SELECT kota, ROUND(SUM(tarif)/1e9, 2) AS rev
    FROM trips WHERE status='completed'
    GROUP BY kota ORDER BY rev DESC
""").df()

plt.figure(figsize=(10, 5))
plt.barh(kota_rev['kota'][::-1], kota_rev['rev'][::-1], color='#EA4335')
plt.xlabel('Revenue (Miliar Rp)')
plt.title('Revenue per Kota')
for i, v in enumerate(kota_rev['rev'][::-1]):
    plt.text(v + 0.1, i, f'{v}', va='center')
plt.tight_layout()
plt.show()

# %% [14] Cleanup
con.close()
print("✅ Praktikum selesai!")
