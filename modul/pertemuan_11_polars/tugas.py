# ==============================================================================
# PERTEMUAN 11 — TUGAS: Polars
# Gunakan dataset e-commerce dari praktikum (/tmp/ecommerce.parquet)
# ==============================================================================
import polars as pl

df = pl.read_parquet('/tmp/ecommerce.parquet')

# TUGAS 1 (Mudah): Pola Hari Belanja
# ------------------------------------
# Hari apa orang paling banyak belanja online?
# Tampilkan: nama_hari, orders, avg_harga, revenue
# Gunakan .dt.weekday() (0=Senin, 6=Minggu)
# Gunakan Polars lazy mode!

result = (
    df.lazy()
    # TODO: filter status delivered
    # TODO: extract weekday, map ke nama hari
    # TODO: group_by, agg, sort
    .collect()
)


# TUGAS 2 (Sedang): Market Basket — Top Kategori per Kota
# --------------------------------------------------------
# Untuk setiap kota, tampilkan:
#   1. Kategori #1 paling populer
#   2. Kategori #2
#   3. Payment method paling populer
#   4. Rata-rata rating
#
# Hint: Gunakan Polars expressions dan group_by

result2 = (
    df.lazy()
    # TODO
    .collect()
)


# TUGAS 3 (Menantang): Customer Lifetime Value (CLV)
# ---------------------------------------------------
# CLV = AOV × Purchase_Frequency × Avg_Lifespan
# Dimana:
#   AOV = total_spend / total_orders
#   Purchase_Frequency = total_orders / active_months
#   Avg_Lifespan = last_month - first_month + 1
#
# Untuk setiap user, hitung CLV.
# Segmentasi: Low (<500K), Medium (500K-2M), High (2M-10M), VIP (>10M)
# Berapa jumlah user di masing-masing segment?

result3 = (
    df.lazy()
    # TODO
    .collect()
)


# TUGAS BONUS: Polars + DuckDB hybrid
# ------------------------------------
# Gunakan DuckDB untuk SQL analytics dan Polars untuk wrangling.
# Pattern: query SQL di DuckDB → convert ke Polars → lanjut analisis.
# Buat analisis menarik yang menggabungkan keduanya.
