# ==============================================================================
# PERTEMUAN 16 — Interactive Dashboard + Capstone
# Platform: Google Colab (demo) + Streamlit (dashboard)
#
# BAGIAN A: Streamlit Dashboard (demo di Colab, deploy di Streamlit Cloud)
# BAGIAN B: Capstone Project Template
# ==============================================================================

# %% [1] Setup
!pip install duckdb polars streamlit plotly -q

import duckdb
import polars as pl
import time, os

print("✅ Ready!")

# %% [2] Generate Dataset Final (gabungan semua konsep)
con = duckdb.connect('/tmp/dashboard.duckdb')
NUM = 2_000_000

con.sql(f"""
CREATE OR REPLACE TABLE sales AS
WITH rng AS (
    SELECT row_number() OVER () AS rn,
           random() AS r1, random() AS r2, random() AS r_user,
           random() AS r_kat, random() AS r_harga, random() AS r_harga_v,
           random() AS r_qty, random() AS r_qty_v, random() AS r_kota,
           random() AS r_pay, random() AS r_rating, random() AS r_status, random() AS r_plat
    FROM generate_series(1, {NUM})
)
SELECT
    'TXN' || LPAD(CAST(rn AS VARCHAR), 8, '0') AS txn_id,
    TIMESTAMP '2024-01-01' + INTERVAL (r1*365) DAY + INTERVAL (r2*24) HOUR AS waktu,
    'U' || LPAD(CAST(FLOOR(r_user*50000+1) AS INT)::VARCHAR, 5, '0') AS user_id,
    CASE WHEN r_kat<.20 THEN 'Fashion' WHEN r_kat<.35 THEN 'Elektronik'
         WHEN r_kat<.50 THEN 'F&B' WHEN r_kat<.65 THEN 'Kecantikan'
         WHEN r_kat<.80 THEN 'Rumah Tangga' ELSE 'Olahraga' END AS kategori,
    ROUND(CASE WHEN r_harga<.3 THEN 25e3+r_harga_v*475e3
               WHEN r_harga<.5 THEN 100e3+r_harga_v*4.9e6
               ELSE 10e3+r_harga_v*490e3 END, -2) AS harga,
    CASE WHEN r_qty<.6 THEN 1 WHEN r_qty<.85 THEN 2 WHEN r_qty<.95 THEN 3
         ELSE FLOOR(4+r_qty_v*5)::INT END AS qty,
    CASE WHEN r_kota<.25 THEN 'Jakarta' WHEN r_kota<.40 THEN 'Surabaya'
         WHEN r_kota<.52 THEN 'Bandung' WHEN r_kota<.60 THEN 'Medan'
         WHEN r_kota<.68 THEN 'Semarang' WHEN r_kota<.75 THEN 'Yogya'
         WHEN r_kota<.82 THEN 'Makassar' WHEN r_kota<.88 THEN 'Denpasar'
         WHEN r_kota<.94 THEN 'Malang' ELSE 'Palembang' END AS kota,
    CASE WHEN r_pay<.30 THEN 'Bank' WHEN r_pay<.55 THEN 'E-Wallet'
         WHEN r_pay<.70 THEN 'COD' WHEN r_pay<.85 THEN 'CC'
         ELSE 'Paylater' END AS payment,
    CASE WHEN r_rating<.03 THEN 1 WHEN r_rating<.08 THEN 2 WHEN r_rating<.18 THEN 3
         WHEN r_rating<.45 THEN 4 ELSE 5 END AS rating,
    CASE WHEN r_status<.80 THEN 'completed' WHEN r_status<.90 THEN 'shipped'
         WHEN r_status<.95 THEN 'processing' ELSE 'cancelled' END AS status,
    CASE WHEN r_plat<.65 THEN 'mobile' WHEN r_plat<.92 THEN 'web'
         ELSE 'api' END AS platform
FROM rng
""")

# Pre-compute daily summary for dashboard
con.sql("""
CREATE OR REPLACE TABLE daily_summary AS
SELECT
    waktu::DATE AS tanggal,
    COUNT(*) AS orders,
    SUM(harga * qty) AS revenue,
    SUM(CASE WHEN status='completed' THEN harga*qty ELSE 0 END) AS completed_rev,
    COUNT(DISTINCT user_id) AS buyers,
    ROUND(AVG(CASE WHEN rating BETWEEN 1 AND 5 THEN rating END), 2) AS avg_rating,
    SUM(CASE WHEN platform='mobile' THEN 1 ELSE 0 END) AS mobile,
    SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancellations
FROM sales
GROUP BY tanggal ORDER BY tanggal
""")

# Pre-compute category summary
con.sql("""
CREATE OR REPLACE TABLE category_summary AS
SELECT
    kategori,
    COUNT(*) AS orders,
    SUM(harga*qty) AS revenue,
    ROUND(AVG(harga)) AS avg_price,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(DISTINCT user_id) AS buyers
FROM sales WHERE status='completed'
GROUP BY kategori ORDER BY revenue DESC
""")

# City summary
con.sql("""
CREATE OR REPLACE TABLE city_summary AS
SELECT
    kota,
    COUNT(*) AS orders,
    SUM(harga*qty) AS revenue,
    COUNT(DISTINCT user_id) AS buyers,
    ROUND(AVG(rating), 2) AS avg_rating
FROM sales WHERE status='completed'
GROUP BY kota ORDER BY revenue DESC
""")

print(f"✅ Dataset: {NUM:,} rows + summary tables created")
con.close()

# %% [3] Streamlit Dashboard Code
# Simpan sebagai file .py untuk dijalankan dengan `streamlit run`

dashboard_code = '''
import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ─── Config ───
st.set_page_config(page_title="TokoKita Dashboard", layout="wide")
con = duckdb.connect('/tmp/dashboard.duckdb', read_only=True)

# ─── Header ───
st.title("📊 TokoKita Analytics Dashboard")
st.caption("Modern Big Data Dashboard — DuckDB + Streamlit")

# ─── KPI Cards ───
kpi = con.sql("""
    SELECT
        SUM(revenue)/1e9 AS rev_B,
        SUM(orders) AS total_orders,
        SUM(buyers) AS total_buyers,
        AVG(avg_rating) AS avg_rating
    FROM daily_summary
""").fetchone()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue", f"Rp {kpi[0]:.1f} Miliar")
c2.metric("Total Orders", f"{kpi[1]:,.0f}")
c3.metric("Unique Buyers", f"{kpi[2]:,.0f}")
c4.metric("Avg Rating", f"{kpi[3]:.2f} / 5")

st.divider()

# ─── Filters ───
col_f1, col_f2 = st.columns(2)
with col_f1:
    kategori_list = [r[0] for r in con.sql("SELECT DISTINCT kategori FROM sales ORDER BY kategori").fetchall()]
    sel_kat = st.multiselect("Kategori", kategori_list, default=kategori_list)
with col_f2:
    kota_list = [r[0] for r in con.sql("SELECT DISTINCT kota FROM sales ORDER BY kota").fetchall()]
    sel_kota = st.multiselect("Kota", kota_list, default=kota_list[:5])

# Build filter
kat_str = ",".join(f"\\'{k}\\'" for k in sel_kat)
kota_str = ",".join(f"\\'{k}\\'" for k in sel_kota)

# ─── Revenue Trend ───
st.subheader("📈 Revenue Trend")
df_daily = con.sql(f"""
    SELECT waktu::DATE AS tanggal, SUM(harga*qty) AS revenue, COUNT(*) AS orders
    FROM sales
    WHERE status='completed' AND kategori IN ({kat_str}) AND kota IN ({kota_str})
    GROUP BY tanggal ORDER BY tanggal
""").df()

if len(df_daily) > 0:
    df_daily['ma7'] = df_daily['revenue'].rolling(7).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_daily['tanggal'], y=df_daily['revenue']/1e6,
                             mode='lines', name='Daily', opacity=0.3))
    fig.add_trace(go.Scatter(x=df_daily['tanggal'], y=df_daily['ma7']/1e6,
                             mode='lines', name='MA-7', line=dict(width=3)))
    fig.update_layout(yaxis_title='Revenue (Juta Rp)', height=350)
    st.plotly_chart(fig, use_container_width=True)

# ─── Category & City ───
col1, col2 = st.columns(2)

with col1:
    st.subheader("📦 Revenue per Kategori")
    df_cat = con.sql(f"""
        SELECT kategori, SUM(harga*qty)/1e9 AS rev_B
        FROM sales WHERE status='completed' AND kota IN ({kota_str})
        GROUP BY kategori ORDER BY rev_B DESC
    """).df()
    fig_cat = px.bar(df_cat, x='kategori', y='rev_B', color='kategori',
                     labels={'rev_B': 'Revenue (Miliar Rp)'})
    fig_cat.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig_cat, use_container_width=True)

with col2:
    st.subheader("🏙️ Revenue per Kota")
    df_city = con.sql(f"""
        SELECT kota, SUM(harga*qty)/1e9 AS rev_B
        FROM sales WHERE status='completed' AND kategori IN ({kat_str})
        GROUP BY kota ORDER BY rev_B DESC LIMIT 10
    """).df()
    fig_city = px.bar(df_city, x='rev_B', y='kota', orientation='h',
                      labels={'rev_B': 'Revenue (Miliar Rp)'})
    fig_city.update_layout(height=350)
    st.plotly_chart(fig_city, use_container_width=True)

# ─── Hourly Pattern ───
st.subheader("⏰ Pola Jam Belanja")
df_hour = con.sql(f"""
    SELECT EXTRACT(HOUR FROM waktu) AS jam, COUNT(*) AS orders
    FROM sales WHERE status='completed' AND kategori IN ({kat_str})
    GROUP BY jam ORDER BY jam
""").df()
fig_hour = px.bar(df_hour, x='jam', y='orders', labels={'orders': 'Orders', 'jam': 'Jam'})
fig_hour.update_layout(height=300)
st.plotly_chart(fig_hour, use_container_width=True)

# ─── Payment & Platform ───
col3, col4 = st.columns(2)
with col3:
    st.subheader("💳 Metode Pembayaran")
    df_pay = con.sql("SELECT payment, COUNT(*) AS n FROM sales WHERE status='completed' GROUP BY payment").df()
    fig_pay = px.pie(df_pay, values='n', names='payment')
    fig_pay.update_layout(height=300)
    st.plotly_chart(fig_pay, use_container_width=True)

with col4:
    st.subheader("📱 Platform")
    df_plat = con.sql("SELECT platform, COUNT(*) AS n FROM sales WHERE status='completed' GROUP BY platform").df()
    fig_plat = px.pie(df_plat, values='n', names='platform')
    fig_plat.update_layout(height=300)
    st.plotly_chart(fig_plat, use_container_width=True)

# ─── Raw Data Explorer ───
with st.expander("🔍 Explore Raw Data"):
    query = st.text_area("SQL Query", "SELECT * FROM sales LIMIT 100")
    if st.button("Run"):
        try:
            result = con.sql(query).df()
            st.dataframe(result)
            st.caption(f"{len(result)} rows returned")
        except Exception as e:
            st.error(str(e))

st.caption("Built with DuckDB + Streamlit | Modern Big Data Stack 2025")
con.close()
'''

with open('/tmp/dashboard.py', 'w') as f:
    f.write(dashboard_code)

print("✅ Dashboard code saved to /tmp/dashboard.py")
print("\nUntuk menjalankan locally:")
print("  streamlit run /tmp/dashboard.py")
print("\nUntuk deploy gratis:")
print("  1. Push ke GitHub repo")
print("  2. share.streamlit.io → deploy dari repo")
print("  3. Atau: huggingface.co/spaces → Streamlit Space")

# %% [4] Preview Dashboard di Colab (static charts)
import matplotlib.pyplot as plt

con = duckdb.connect('/tmp/dashboard.duckdb', read_only=True)

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('TokoKita Analytics Dashboard (Preview)', fontsize=14, fontweight='bold')

# Revenue trend
daily = con.sql("""
    SELECT tanggal, revenue/1e9 AS rev,
           AVG(revenue/1e9) OVER (ORDER BY tanggal ROWS 6 PRECEDING) AS ma7
    FROM daily_summary
""").df()
axes[0][0].plot(daily['tanggal'], daily['rev'], alpha=.3)
axes[0][0].plot(daily['tanggal'], daily['ma7'], color='red', lw=2)
axes[0][0].set_title('Daily Revenue (Miliar Rp)'); axes[0][0].grid(alpha=.3)

# Category
cat = con.sql("SELECT kategori, revenue/1e9 AS rev FROM category_summary").df()
axes[0][1].barh(cat['kategori'][::-1], cat['rev'][::-1], color='#4285F4')
axes[0][1].set_title('Revenue per Kategori (Miliar Rp)')

# City
city = con.sql("SELECT kota, revenue/1e9 AS rev FROM city_summary").df()
axes[1][0].barh(city['kota'][::-1], city['rev'][::-1], color='#34A853')
axes[1][0].set_title('Revenue per Kota (Miliar Rp)')

# Hourly
hourly = con.sql("""
    SELECT EXTRACT(HOUR FROM waktu) AS jam, COUNT(*) AS orders
    FROM sales WHERE status='completed' GROUP BY jam ORDER BY jam
""").df()
axes[1][1].bar(hourly['jam'], hourly['orders'], color='#EA4335')
axes[1][1].set_title('Orders per Jam'); axes[1][1].set_xlabel('Jam')

plt.tight_layout(); plt.show()
con.close()

# %% [5] Capstone Project — Template
print("""
╔══════════════════════════════════════════════════════════╗
║           CAPSTONE PROJECT TEMPLATE                      ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  Pilih SATU domain:                                      ║
║                                                          ║
║  A. E-Commerce Analytics Platform                        ║
║     Raw data → DuckDB → dbt → Dashboard + Chatbot       ║
║                                                          ║
║  B. IoT Monitoring Dashboard                             ║
║     Sensor stream → anomaly detection → alert dashboard  ║
║                                                          ║
║  C. Social Media Intelligence                            ║
║     Event stream → sentiment analysis → dashboard        ║
║                                                          ║
║  Deliverables:                                           ║
║  1. Architecture diagram                                 ║
║  2. Working code (Colab notebook)                        ║
║  3. Presentation (10 min)                                ║
║  4. Live demo                                            ║
║                                                          ║
║  Grading:                                                ║
║  - Architecture & design  : 30%                          ║
║  - Implementation         : 40%                          ║
║  - Presentation & demo    : 20%                          ║
║  - Creativity             : 10%                          ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
""")

# %% [6] Capstone Starter Code — Option A: E-Commerce Platform
capstone_code = '''
# ==============================================================================
# CAPSTONE PROJECT: E-Commerce Analytics Platform
# Menggabungkan semua konsep dari Pertemuan 9-16
# ==============================================================================

# --- Step 1: Data Ingestion (Pertemuan 9) ---
# Generate atau download dataset
import duckdb
con = duckdb.connect('capstone.duckdb')
# TODO: Create raw tables (orders, products, customers)

# --- Step 2: Data Processing (Pertemuan 11 - Polars) ---
import polars as pl
# TODO: Clean data, handle nulls, fix inconsistencies

# --- Step 3: Data Transformation (Pertemuan 12 - dbt) ---
# TODO: Create dbt project with staging → intermediate → marts

# --- Step 4: Storage (Pertemuan 14 - Lakehouse) ---
# TODO: Save as Parquet with partitioning
# TODO: Implement time travel / snapshots

# --- Step 5: Analytics (Pertemuan 9, 10, 11) ---
# TODO: Run analytical queries (SQL + Polars)
# KPIs: revenue, orders, customers, retention, AOV

# --- Step 6: AI Integration (Pertemuan 15 - RAG) ---
import chromadb
# TODO: Build knowledge base from analytics results
# TODO: Build chatbot that answers questions about the data

# --- Step 7: Dashboard (Pertemuan 16 - Streamlit) ---
# TODO: Build interactive dashboard
# Combine: KPI cards, charts, SQL explorer, chatbot
'''

with open('/tmp/capstone_template.py', 'w') as f:
    f.write(capstone_code)
print("✅ Capstone template saved to /tmp/capstone_template.py")

# %% [7] Ringkasan Seluruh Mata Kuliah
print("""
📚 RINGKASAN PERTEMUAN 9-16: MODERN BIG DATA ENGINEERING

┌──────┬───────────────────────────┬────────────────────────────┐
│  #   │ Topik                     │ Tools                      │
├──────┼───────────────────────────┼────────────────────────────┤
│  9   │ DuckDB + Parquet          │ DuckDB, PyArrow            │
│  10  │ BigQuery (Cloud SQL)      │ Google BigQuery             │
│  11  │ Polars (DataFrame)        │ Polars, lazy eval           │
│  12  │ dbt (Data Pipeline)       │ dbt-core, DuckDB            │
│  13  │ Stream Processing         │ Python (Kafka concepts)     │
│  14  │ Data Lakehouse            │ Parquet, Iceberg concepts   │
│  15  │ RAG + Vector DB           │ ChromaDB, Transformers      │
│  16  │ Dashboard + Capstone      │ Streamlit, DuckDB           │
└──────┴───────────────────────────┴────────────────────────────┘

Modern Data Stack 2025:
  Query   : DuckDB, BigQuery  (bukan Hive)
  DataFrame: Polars            (bukan Pandas untuk big data)
  Pipeline : dbt               (bukan custom Spark ETL)
  Storage  : Parquet + Iceberg (bukan HDFS)
  AI       : RAG + Vector DB   (baru!)
  Dashboard: Streamlit          (gratis!)
""")
print("✅ Praktikum selesai! Selamat menyelesaikan capstone project 🎓")
