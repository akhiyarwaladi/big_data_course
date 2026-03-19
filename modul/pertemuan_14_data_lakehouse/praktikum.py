# ==============================================================================
# PERTEMUAN 14 — Data Lakehouse Architecture
# Studi Kasus: Data Lake Perusahaan Logistik "NusantaraLogistik"
# Platform: Google Colab + DuckDB + Parquet
# ==============================================================================

# %% [1] Setup
!pip install duckdb polars pyarrow -q

import duckdb, polars as pl, pyarrow.parquet as pq
import os, json, shutil, time
from datetime import datetime

LAKE = '/content/nusantara_lake'
if os.path.exists(LAKE): shutil.rmtree(LAKE)
for d in ['bronze/shipments','silver/shipments','gold/reports',
          'metadata/snapshots']:
    os.makedirs(f'{LAKE}/{d}', exist_ok=True)

print("📁 Data Lake Structure (Medallion):")
!find {LAKE} -type d

# %% [2] Generate BRONZE layer — raw shipment data (partitioned by month)
con = duckdb.connect()
total_rows = 0
for m in range(1, 13):
    n = 50_000 + int(10_000 * m / 12)
    os.makedirs(f'{LAKE}/bronze/shipments/month={m:02d}', exist_ok=True)
    con.sql(f"""
    COPY (
      WITH rng AS (
        SELECT row_number() OVER () AS rn,
               random() AS r_cust, random() AS r_orig, random() AS r_dest,
               random() AS r_wt, random() AS r_svc, random() AS r_cost, random() AS r_cost_v,
               random() AS r_date, random() AS r_stat, random() AS r_days, random() AS r_ins
        FROM generate_series(1, {n})
      )
      SELECT
        'SHP' || LPAD(CAST(rn + ({m}*100000) AS VARCHAR), 10, '0') AS shipment_id,
        'C' || LPAD(CAST(FLOOR(r_cust*20000+1) AS INT)::VARCHAR, 6, '0') AS customer_id,
        CASE WHEN r_orig<.30 THEN 'Jakarta' WHEN r_orig<.50 THEN 'Surabaya'
             WHEN r_orig<.65 THEN 'Bandung' WHEN r_orig<.75 THEN 'Semarang'
             WHEN r_orig<.85 THEN 'Medan' ELSE 'Makassar' END AS origin,
        CASE WHEN r_dest<.25 THEN 'Jakarta' WHEN r_dest<.45 THEN 'Surabaya'
             WHEN r_dest<.60 THEN 'Bandung' WHEN r_dest<.72 THEN 'Yogya'
             WHEN r_dest<.82 THEN 'Denpasar' ELSE 'Manado' END AS destination,
        ROUND(0.1+r_wt*30, 2) AS weight_kg,
        CASE WHEN r_svc<.40 THEN 'regular' WHEN r_svc<.70 THEN 'express'
             WHEN r_svc<.90 THEN 'same_day' ELSE 'cargo' END AS service,
        ROUND(CASE WHEN r_cost<.40 THEN 8e3+r_cost_v*42e3
                   WHEN r_cost<.70 THEN 15e3+r_cost_v*85e3
                   WHEN r_cost<.90 THEN 25e3+r_cost_v*75e3
                   ELSE 50e3+r_cost_v*450e3 END, -2) AS cost,
        DATE '2024-{m:02d}-01' + INTERVAL (FLOOR(r_date*28)::INT) DAY AS ship_date,
        CASE WHEN r_stat<.80 THEN 'delivered' WHEN r_stat<.90 THEN 'in_transit'
             WHEN r_stat<.95 THEN 'returned' ELSE 'lost' END AS status,
        FLOOR(1+r_days*5)::INT AS delivery_days,
        r_ins<.30 AS insured
      FROM rng
    ) TO '{LAKE}/bronze/shipments/month={m:02d}/data.parquet' (FORMAT PARQUET)
    """)
    total_rows += n

print(f"✅ BRONZE: {total_rows:,} rows across 12 monthly partitions")

# %% [3] Understand Partitioning — Benchmark
print("📂 PARTITION PRUNING BENCHMARK")
print("=" * 50)

t0 = time.time()
r_all = con.sql(f"""
    SELECT COUNT(*), SUM(cost) FROM read_parquet(
        '{LAKE}/bronze/shipments/*/data.parquet', hive_partitioning=true)
""").fetchone()
t_all = time.time() - t0

t0 = time.time()
r_jan = con.sql(f"""
    SELECT COUNT(*), SUM(cost) FROM read_parquet(
        '{LAKE}/bronze/shipments/*/data.parquet', hive_partitioning=true)
    WHERE month = '01'
""").fetchone()
t_jan = time.time() - t0

print(f"Full scan   : {t_all:.4f}s | {r_all[0]:,} rows")
print(f"January only: {t_jan:.4f}s | {r_jan[0]:,} rows")
print(f"Partition pruning → {t_all/t_jan:.1f}x faster!")

# Column pruning
t0 = time.time()
con.sql(f"SELECT * FROM read_parquet('{LAKE}/bronze/shipments/month=01/data.parquet') LIMIT 100000").fetchall()
t_star = time.time() - t0

t0 = time.time()
con.sql(f"SELECT origin, cost FROM read_parquet('{LAKE}/bronze/shipments/month=01/data.parquet') LIMIT 100000").fetchall()
t_cols = time.time() - t0

print(f"\nSELECT *      : {t_star:.4f}s")
print(f"SELECT 2 cols : {t_cols:.4f}s → {t_star/t_cols:.1f}x faster (column pruning)")

# %% [4] Build SILVER layer — Time Travel with Snapshots
class Lakehouse:
    """Mini lakehouse: snapshots, time travel, ACID, schema evolution."""
    def __init__(self, path):
        self.path = path
        self.snapshots = []
        self.current = None
        os.makedirs(f'{path}/data', exist_ok=True)
        os.makedirs(f'{path}/meta', exist_ok=True)

    def write(self, df, desc=""):
        sid = len(self.snapshots) + 1
        fpath = f'{self.path}/data/snap_{sid:04d}.parquet'
        df.write_parquet(fpath)
        snap = {
            'id': sid, 'ts': datetime.now().isoformat(), 'file': fpath,
            'desc': desc, 'rows': len(df), 'cols': df.columns,
            'mb': round(os.path.getsize(fpath)/1e6, 2),
        }
        self.snapshots.append(snap)
        self.current = sid
        with open(f'{self.path}/meta/snapshots.json', 'w') as f:
            json.dump(self.snapshots, f, indent=2)
        print(f"✅ Snapshot {sid}: {desc} ({len(df):,} rows, {snap['mb']} MB)")

    def read(self, sid=None):
        sid = sid or self.current
        return pl.read_parquet(self.snapshots[sid-1]['file'])

    def history(self):
        print(f"\n📜 HISTORY ({len(self.snapshots)} snapshots)")
        for s in self.snapshots:
            cur = " ← CURRENT" if s['id'] == self.current else ""
            print(f"  v{s['id']} | {s['ts'][:19]} | {s['rows']:>8,} rows | {s['desc']}{cur}")

    def rollback(self, sid):
        self.current = sid
        print(f"⏪ Rolled back to v{sid}")

lh = Lakehouse(f'{LAKE}/silver/shipments')

# Version 1: January
df_v1 = pl.read_parquet(f'{LAKE}/bronze/shipments/month=01/data.parquet')
lh.write(df_v1, "Initial load — January")

# Version 2: + February
df_feb = pl.read_parquet(f'{LAKE}/bronze/shipments/month=02/data.parquet')
df_v2 = pl.concat([df_v1, df_feb])
lh.write(df_v2, "Added February")

# Version 3: Data cleaning — remove 'lost'
df_v3 = df_v2.filter(pl.col('status') != 'lost')
lh.write(df_v3, "Removed lost shipments")

# Version 4: Schema evolution — add tax column
df_v4 = df_v3.with_columns(
    (pl.col('cost') * 0.11).round(0).alias('tax'),
    pl.lit('IDR').alias('currency'),
)
lh.write(df_v4, "Schema evolution: +tax, +currency")

lh.history()

# %% [5] Time Travel Queries
print("\n🕐 TIME TRAVEL: Compare versions")
for v in [1, 2, 3, 4]:
    d = lh.read(v)
    print(f"  v{v}: {len(d):,} rows | cols: {d.columns}")

print(f"\nv2 → v3: removed {len(lh.read(2)) - len(lh.read(3)):,} 'lost' rows")
print(f"v3 → v4: added columns {set(lh.read(4).columns) - set(lh.read(3).columns)}")

# %% [6] GOLD layer — Analytics
con2 = duckdb.connect()
df_current = lh.read()
con2.register('shipments', df_current)

print("\n📊 GOLD: NusantaraLogistik Analytics")
print("=" * 55)

print("\n🚚 Top Routes:")
con2.sql("""
    SELECT origin || ' → ' || destination AS route,
           COUNT(*) AS shipments, ROUND(SUM(cost)/1e6, 1) AS rev_jt,
           ROUND(AVG(cost)) AS avg_cost, ROUND(AVG(delivery_days), 1) AS avg_days
    FROM shipments WHERE status='delivered'
    GROUP BY route ORDER BY rev_jt DESC LIMIT 10
""").show()

print("\n📦 Service Performance:")
con2.sql("""
    SELECT service, COUNT(*) AS total,
           ROUND(AVG(cost)) AS avg_cost,
           ROUND(AVG(delivery_days), 1) AS avg_days,
           ROUND(SUM(insured::INT)*100.0/COUNT(*), 1) AS insured_pct,
           ROUND(COUNT(*) FILTER (WHERE status='delivered')*100.0/COUNT(*), 1) AS deliver_pct
    FROM shipments GROUP BY service ORDER BY total DESC
""").show()

print("\n⏱️ SLA Compliance:")
con2.sql("""
    SELECT service,
           CASE service WHEN 'same_day' THEN 1 WHEN 'express' THEN 2
                        WHEN 'regular' THEN 5 WHEN 'cargo' THEN 7 END AS sla_days,
           COUNT(*) AS delivered,
           ROUND(AVG(delivery_days), 1) AS actual_avg,
           ROUND(SUM(CASE
               WHEN service='same_day' AND delivery_days<=1 THEN 1
               WHEN service='express' AND delivery_days<=2 THEN 1
               WHEN service='regular' AND delivery_days<=5 THEN 1
               WHEN service='cargo' AND delivery_days<=7 THEN 1
               ELSE 0 END)*100.0/COUNT(*), 1) AS sla_pct
    FROM shipments WHERE status='delivered' GROUP BY service
""").show()
con2.close()

# %% [7] Data Quality Framework
print("\n🔍 DATA QUALITY CHECKS")
print("=" * 50)

df = lh.read()
checks = []

# Null check
for col in df.columns:
    n = df.filter(pl.col(col).is_null()).height
    if n > 0:
        checks.append(('Null: ' + col, 'WARN' if n/len(df)<.05 else 'FAIL',
                        f'{n:,} ({n/len(df)*100:.2f}%)'))

# Negative cost
n_neg = df.filter(pl.col('cost') < 0).height
checks.append(('Negative cost', 'PASS' if n_neg==0 else 'FAIL', f'{n_neg}'))

# Delivery days range
n_bad = df.filter((pl.col('delivery_days') < 0) | (pl.col('delivery_days') > 30)).height
checks.append(('Delivery days [0-30]', 'PASS' if n_bad==0 else 'WARN', f'{n_bad}'))

# Row count
checks.append(('Row count > 80K', 'PASS' if len(df)>80000 else 'FAIL', f'{len(df):,}'))

for name, status, detail in checks:
    icon = {'PASS':'✅','WARN':'⚠️','FAIL':'❌'}[status]
    print(f"  {icon} {name:30s} {status:5s} {detail}")

passed = sum(1 for _,s,_ in checks if s=='PASS')
print(f"\nScore: {passed}/{len(checks)} passed")

# %% [8] Medallion Architecture Diagram
print("""
📊 MEDALLION ARCHITECTURE — NusantaraLogistik

┌───────────────────────────────────────────────────┐
│ BRONZE (Raw)          SILVER (Cleaned)    GOLD    │
│                                                   │
│ bronze/shipments/ → silver/shipments/  → gold/    │
│   month=01/           snapshots +          reports │
│   month=02/           time travel          marts  │
│   ...                 schema evolution     dash   │
│                                                   │
│ Parquet files    Parquet + metadata    DuckDB/SQL │
│ Partitioned      Versioned             Aggregated │
│ As-is from src   Cleaned & enriched    Ready use  │
└───────────────────────────────────────────────────┘
""")
print("✅ Praktikum selesai!")
