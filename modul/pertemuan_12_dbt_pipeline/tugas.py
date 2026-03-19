# ==============================================================================
# PERTEMUAN 12 — TUGAS: dbt Pipeline
# Lanjutkan project dbt dari praktikum
# ==============================================================================

# TUGAS 1 (Mudah): Buat mart model baru — rpt_category_performance.sql
# ---------------------------------------------------------------------
# Tambahkan file models/marts/rpt_category_performance.sql
# Isi: performa setiap kategori produk
# Columns: category, total_orders, total_revenue, total_profit,
#          profit_margin_pct, avg_rating, top_payment
# Gunakan {{ ref('fct_sales') }}

"""
-- models/marts/rpt_category_performance.sql
SELECT
    category,
    -- TODO: lengkapi aggregations
FROM {{ ref('fct_sales') }}
GROUP BY category
ORDER BY total_revenue DESC
"""


# TUGAS 2 (Sedang): Custom test + Macro
# ----------------------------------------
# a) Buat custom test: tests/assert_no_future_orders.sql
#    Cek bahwa tidak ada order dengan tanggal di masa depan

"""
-- tests/assert_no_future_orders.sql
SELECT *
FROM {{ ref('fct_sales') }}
WHERE sale_date > CURRENT_DATE
"""

# b) Buat macro: macros/rupiah.sql
#    Format angka ke rupiah: {{ rupiah('revenue') }} → 'Rp 1,234,567'
#    Lalu gunakan di salah satu mart model

"""
-- macros/rupiah.sql
{% macro rupiah(column) %}
    'Rp ' || TO_CHAR({{ column }}, 'FM999,999,999,999')
{% endmacro %}
"""


# TUGAS 3 (Menantang): Incremental Model
# -----------------------------------------
# Buat model incremental yang hanya memproses data baru (bukan semua).
# Ini penting di production karena hemat waktu!
#
# File: models/marts/fct_sales_incremental.sql
# Materialization: incremental
# Unique key: order_id

"""
-- models/marts/fct_sales_incremental.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT
    order_id, customer_id, product_id, category,
    revenue, profit, sale_date, status
FROM {{ ref('int_order_items') }}
WHERE status = 'completed'

{% if is_incremental() %}
    -- Hanya proses data baru sejak last run
    AND order_date > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
"""

# Setelah membuat file-file di atas, jalankan:
# !dbt run
# !dbt test
