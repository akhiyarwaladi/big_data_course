# ==============================================================================
# PERTEMUAN 11 — Polars: High-Performance DataFrame
# Studi Kasus: Analisis 10 Juta Transaksi E-Commerce Indonesia
# Platform: Google Colab
# ==============================================================================

# %% [1] Setup
!pip install polars duckdb pyarrow matplotlib -q

import polars as pl
import duckdb
import pandas as pd
import time, os

print(f"Polars {pl.__version__} | CPU cores: {os.cpu_count()}")

# %% [2] Generate Dataset E-Commerce (10 Juta order) via DuckDB
NUM = 10_000_000
con = duckdb.connect()

t0 = time.time()
con.sql(f"""
COPY (
  WITH rng AS (
    SELECT
      row_number() OVER () AS rn,
      random() AS r1, random() AS r2, random() AS r_user,
      random() AS r_kat, random() AS r_harga, random() AS r_harga_v,
      random() AS r_qty, random() AS r_qty_v,
      random() AS r_kota, random() AS r_pay, random() AS r_rating,
      random() AS r_status, random() AS r_ship, random() AS r_plat
    FROM generate_series(1, {NUM})
  )
  SELECT
    'TXN' || LPAD(CAST(rn AS VARCHAR), 10, '0') AS txn_id,
    TIMESTAMP '2024-01-01' + INTERVAL (r1*365) DAY + INTERVAL (r2*24) HOUR AS waktu,
    'U' || LPAD(CAST(FLOOR(r_user*500000+1) AS INT)::VARCHAR, 6, '0') AS user_id,

    CASE WHEN r_kat<.20 THEN 'Fashion'
         WHEN r_kat<.35 THEN 'Elektronik'
         WHEN r_kat<.48 THEN 'F&B'
         WHEN r_kat<.58 THEN 'Kecantikan'
         WHEN r_kat<.67 THEN 'Rumah Tangga'
         WHEN r_kat<.75 THEN 'Olahraga'
         WHEN r_kat<.82 THEN 'Otomotif'
         WHEN r_kat<.88 THEN 'Buku'
         WHEN r_kat<.94 THEN 'Gadget'
         ELSE 'Gaming' END AS kategori,

    ROUND(CASE WHEN r_harga<.20 THEN 25e3+r_harga_v*475e3
               WHEN r_harga<.35 THEN 50e3+r_harga_v*4.95e6
               WHEN r_harga<.48 THEN 5e3+r_harga_v*195e3
               ELSE 10e3+r_harga_v*990e3 END, -2) AS harga,

    CASE WHEN r_qty<.60 THEN 1 WHEN r_qty<.85 THEN 2
         WHEN r_qty<.95 THEN 3 ELSE FLOOR(4+r_qty_v*7)::INT END AS qty,

    CASE WHEN r_kota<.25 THEN 'Jakarta'  WHEN r_kota<.38 THEN 'Surabaya'
         WHEN r_kota<.48 THEN 'Bandung'  WHEN r_kota<.55 THEN 'Medan'
         WHEN r_kota<.61 THEN 'Semarang' WHEN r_kota<.66 THEN 'Makassar'
         WHEN r_kota<.71 THEN 'Yogya'    WHEN r_kota<.76 THEN 'Palembang'
         WHEN r_kota<.81 THEN 'Denpasar' WHEN r_kota<.86 THEN 'Malang'
         WHEN r_kota<.90 THEN 'Bogor'    WHEN r_kota<.94 THEN 'Tangerang'
         ELSE 'Bekasi' END AS kota,

    CASE WHEN r_pay<.25 THEN 'Bank Transfer'
         WHEN r_pay<.50 THEN 'E-Wallet'
         WHEN r_pay<.65 THEN 'VA'
         WHEN r_pay<.78 THEN 'COD'
         WHEN r_pay<.90 THEN 'Credit Card'
         ELSE 'Paylater' END AS payment,

    CASE WHEN r_rating<.03 THEN 1 WHEN r_rating<.08 THEN 2
         WHEN r_rating<.18 THEN 3 WHEN r_rating<.45 THEN 4
         ELSE 5 END AS rating,

    CASE WHEN r_status<.78 THEN 'delivered'
         WHEN r_status<.88 THEN 'shipped'
         WHEN r_status<.93 THEN 'processing'
         WHEN r_status<.97 THEN 'cancelled'
         ELSE 'returned' END AS status,

    r_ship<.40 AS free_shipping,

    CASE WHEN r_plat<.65 THEN 'mobile' WHEN r_plat<.92 THEN 'web'
         ELSE 'api' END AS platform

  FROM rng
) TO '/tmp/ecommerce.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print(f"✅ {NUM:,} rows → {os.path.getsize('/tmp/ecommerce.parquet')/1e6:.0f} MB in {time.time()-t0:.1f}s")
con.close()

# %% [3] Load dengan Polars
t0 = time.time()
df = pl.read_parquet('/tmp/ecommerce.parquet')
print(f"Loaded {df.shape[0]:,} rows × {df.shape[1]} cols in {time.time()-t0:.2f}s")
print(f"Memory: {df.estimated_size('mb'):.0f} MB\n")
print(df.schema)
df.head(5)

# %% [4] Polars Basics — Select, Filter, Sort
# Top 10 transaksi Elektronik termahal di Jakarta
(
    df
    .filter(
        (pl.col('kategori') == 'Elektronik')
        & (pl.col('kota') == 'Jakarta')
        & (pl.col('status') == 'delivered')
    )
    .with_columns((pl.col('harga') * pl.col('qty')).alias('total'))
    .select('txn_id', 'harga', 'qty', 'total', 'rating', 'payment')
    .sort('total', descending=True)
    .head(10)
)

# %% [5] Group By + Expressions
print("🏙️ Revenue per Kota:")
(
    df
    .filter(pl.col('status') == 'delivered')
    .group_by('kota')
    .agg(
        pl.len().alias('orders'),
        pl.col('harga').mean().round(0).alias('avg_harga'),
        (pl.col('harga') * pl.col('qty')).sum().alias('revenue'),
        pl.col('rating').mean().round(2).alias('avg_rating'),
        pl.col('user_id').n_unique().alias('unique_users'),
        pl.col('free_shipping').mean().round(3).alias('free_ship_pct'),
    )
    .with_columns((pl.col('revenue') / 1e9).round(2).alias('rev_miliar'))
    .sort('rev_miliar', descending=True)
)

# %% [6] Multi-dimensional Group By
(
    df
    .filter(pl.col('status') == 'delivered')
    .group_by('kategori', 'payment')
    .agg(
        pl.len().alias('n'),
        pl.col('harga').mean().round(0).alias('avg_harga'),
        pl.col('harga').median().alias('median'),
        pl.col('rating').mean().round(2).alias('avg_rating'),
    )
    .sort('kategori', 'n', descending=[False, True])
    .head(20)
)

# %% [7] LAZY EVALUATION — Polars Superpower
print("🦥 Lazy vs Eager benchmark")
print("=" * 50)

# Eager
t0 = time.time()
r1 = (
    df
    .filter(pl.col('status') == 'delivered')
    .filter(pl.col('kategori') == 'Elektronik')
    .group_by('kota')
    .agg(pl.col('harga').mean().alias('avg'))
    .sort('avg', descending=True)
    .head(10)
)
t_eager = time.time() - t0

# Lazy
t0 = time.time()
r2 = (
    df.lazy()
    .filter(pl.col('status') == 'delivered')
    .filter(pl.col('kategori') == 'Elektronik')
    .group_by('kota')
    .agg(pl.col('harga').mean().alias('avg'))
    .sort('avg', descending=True)
    .head(10)
    .collect()
)
t_lazy = time.time() - t0

print(f"Eager: {t_eager:.4f}s")
print(f"Lazy : {t_lazy:.4f}s  ({t_eager/t_lazy:.1f}x faster)")

# Lihat query plan yang dioptimasi
print("\n📋 Optimized Query Plan:")
print(
    df.lazy()
    .filter(pl.col('status') == 'delivered')
    .filter(pl.col('kategori') == 'Elektronik')
    .group_by('kota').agg(pl.col('harga').mean().alias('avg'))
    .sort('avg', descending=True).head(10)
    .explain()
)

# %% [8] scan_parquet — Ultimate Lazy (baca langsung dari file)
print("\n📂 scan_parquet vs read_parquet")

t0 = time.time()
r_read = (
    pl.read_parquet('/tmp/ecommerce.parquet')
    .filter(pl.col('kategori') == 'Gaming')
    .select('txn_id', 'harga', 'kota')
)
t_read = time.time() - t0

t0 = time.time()
r_scan = (
    pl.scan_parquet('/tmp/ecommerce.parquet')
    .filter(pl.col('kategori') == 'Gaming')
    .select('txn_id', 'harga', 'kota')
    .collect()
)
t_scan = time.time() - t0

print(f"read_parquet  : {t_read:.3f}s (baca semua dulu)")
print(f"scan_parquet  : {t_scan:.3f}s (baca hanya yang perlu) → {t_read/t_scan:.0f}x faster")

# %% [9] RFM ANALYSIS — Segmentasi Pelanggan
# R = Recency (kapan terakhir belanja)
# F = Frequency (seberapa sering)
# M = Monetary (berapa total belanja)

print("💎 RFM Customer Segmentation")
print("=" * 50)

rfm = (
    df.lazy()
    .filter(pl.col('status') == 'delivered')
    .group_by('user_id')
    .agg(
        pl.col('waktu').max().alias('last_purchase'),
        pl.len().alias('frequency'),
        (pl.col('harga') * pl.col('qty')).sum().alias('monetary'),
    )
    .with_columns(
        pl.col('frequency').qcut(5, labels=['1','2','3','4','5'], allow_duplicates=True).alias('f_score'),
        pl.col('monetary').qcut(5, labels=['1','2','3','4','5'], allow_duplicates=True).alias('m_score'),
    )
    .with_columns(
        (pl.col('f_score').cast(pl.String) + pl.col('m_score').cast(pl.String)).alias('fm_segment')
    )
    .collect()
)

# Ringkasan segment
print(
    rfm.group_by('fm_segment')
    .agg(
        pl.len().alias('users'),
        pl.col('frequency').mean().round(1).alias('avg_freq'),
        pl.col('monetary').mean().round(0).alias('avg_monetary'),
    )
    .sort('users', descending=True)
    .head(10)
)

# VIP customers (segment "55")
vip = rfm.filter(pl.col('fm_segment') == '55')
print(f"\n🏆 VIP Customers: {len(vip):,}")
print(f"   Avg spend : Rp {vip['monetary'].mean():,.0f}")
print(f"   Avg orders: {vip['frequency'].mean():.1f}")

# %% [10] Cohort Analysis — Distribusi Pelanggan
cohort = (
    df.lazy()
    .filter(pl.col('status') == 'delivered')
    .with_columns(pl.col('waktu').dt.month().alias('bulan'))
    .group_by('user_id')
    .agg(
        pl.col('bulan').min().alias('first_month'),
        pl.col('bulan').n_unique().alias('active_months'),
        pl.len().alias('orders'),
        (pl.col('harga') * pl.col('qty')).sum().alias('total_spend'),
    )
    .collect()
)

print("👥 Customer Segments:")
print(
    cohort
    .group_by(
        pl.when(pl.col('orders') == 1).then(pl.lit('1x buyer'))
        .when(pl.col('orders') <= 3).then(pl.lit('2-3x'))
        .when(pl.col('orders') <= 5).then(pl.lit('4-5x'))
        .when(pl.col('orders') <= 10).then(pl.lit('6-10x'))
        .otherwise(pl.lit('10+ loyal'))
        .alias('segment')
    )
    .agg(
        pl.len().alias('users'),
        pl.col('total_spend').mean().round(0).alias('avg_spend'),
        pl.col('active_months').mean().round(1).alias('avg_months'),
    )
    .sort('users', descending=True)
)

# %% [11] Time Series — Trend + Moving Average + Visualisasi
import matplotlib.pyplot as plt

daily = (
    df.lazy()
    .filter(pl.col('status') == 'delivered')
    .with_columns(pl.col('waktu').dt.date().alias('tanggal'))
    .group_by('tanggal')
    .agg(
        pl.len().alias('orders'),
        (pl.col('harga') * pl.col('qty')).sum().alias('revenue'),
        pl.col('user_id').n_unique().alias('buyers'),
    )
    .sort('tanggal')
    .with_columns(
        pl.col('revenue').rolling_mean(window_size=7).alias('ma7'),
        pl.col('revenue').rolling_mean(window_size=30).alias('ma30'),
    )
    .collect()
).to_pandas()

fig, ax = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

ax[0].plot(daily['tanggal'], daily['revenue']/1e9, alpha=.25, color='blue')
ax[0].plot(daily['tanggal'], daily['ma7']/1e9, color='red', lw=2, label='MA-7')
ax[0].plot(daily['tanggal'], daily['ma30']/1e9, color='green', lw=2, label='MA-30')
ax[0].set_ylabel('Revenue (Miliar Rp)'); ax[0].legend(); ax[0].set_title('Daily Revenue')
ax[0].grid(alpha=.3)

ax[1].plot(daily['tanggal'], daily['buyers'], alpha=.3, color='purple')
ax[1].plot(daily['tanggal'], pd.Series(daily['buyers']).rolling(7).mean(), color='orange', lw=2)
ax[1].set_ylabel('Unique Buyers'); ax[1].set_title('Daily Unique Buyers'); ax[1].grid(alpha=.3)

plt.tight_layout(); plt.show()

# %% [12] BENCHMARK: Pandas vs Polars vs DuckDB
print("🏎️ ULTIMATE BENCHMARK")
print(f"Dataset: {NUM:,} rows")
print("=" * 55)

df_pd = pd.read_parquet('/tmp/ecommerce.parquet')
df_pl = pl.read_parquet('/tmp/ecommerce.parquet')
db = duckdb.connect()

for name, fn_pd, fn_pl, fn_db in [
    ("Filter + GroupBy",
     lambda: df_pd[df_pd['status']=='delivered'].groupby('kategori').agg(n=('harga','count'), avg=('harga','mean')),
     lambda: df_pl.lazy().filter(pl.col('status')=='delivered').group_by('kategori').agg(pl.len(), pl.col('harga').mean()).collect(),
     lambda: db.sql("SELECT kategori, COUNT(*), AVG(harga) FROM '/tmp/ecommerce.parquet' WHERE status='delivered' GROUP BY kategori").fetchall()
    ),
    ("Multi-step: filter + calc + groupby + sort",
     lambda: df_pd[df_pd['status']=='delivered'].assign(total=df_pd['harga']*df_pd['qty']).groupby(['kota','kategori']).agg(rev=('total','sum')).reset_index().sort_values('rev', ascending=False).head(20),
     lambda: df_pl.lazy().filter(pl.col('status')=='delivered').with_columns((pl.col('harga')*pl.col('qty')).alias('total')).group_by('kota','kategori').agg(pl.col('total').sum().alias('rev')).sort('rev', descending=True).head(20).collect(),
     lambda: db.sql("SELECT kota, kategori, SUM(harga*qty) AS rev FROM '/tmp/ecommerce.parquet' WHERE status='delivered' GROUP BY kota, kategori ORDER BY rev DESC LIMIT 20").fetchall()
    ),
]:
    t0=time.time(); fn_pd(); t_pd=time.time()-t0
    t0=time.time(); fn_pl(); t_pl=time.time()-t0
    t0=time.time(); fn_db(); t_db=time.time()-t0
    print(f"\n{name}:")
    print(f"  Pandas: {t_pd:.4f}s")
    print(f"  Polars: {t_pl:.4f}s  ({t_pd/t_pl:.0f}x)")
    print(f"  DuckDB: {t_db:.4f}s  ({t_pd/t_db:.0f}x)")

db.close()
del df_pd
print("\n✅ Praktikum selesai!")
