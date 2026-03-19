# ==============================================================================
# PERTEMUAN 12 — dbt: Data Pipeline Modern
# Studi Kasus: Pipeline Analytics "TokoKita"
# Platform: Google Colab + dbt-core + DuckDB
# ==============================================================================

# %% [1] Setup
!pip install dbt-duckdb duckdb -q
import duckdb, os, time

!dbt --version | head -2
PROJECT = '/content/tokokita'

# %% [2] Buat dbt project structure
for d in ['models/staging','models/intermediate','models/marts',
          'seeds','tests','macros']:
    os.makedirs(f'{PROJECT}/{d}', exist_ok=True)

# dbt_project.yml
with open(f'{PROJECT}/dbt_project.yml', 'w') as f:
    f.write("""name: tokokita
version: '1.0.0'
profile: tokokita
model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
clean-targets: ["target","dbt_packages"]

models:
  tokokita:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
""")

# profiles.yml
os.makedirs(os.path.expanduser('~/.dbt'), exist_ok=True)
with open(os.path.expanduser('~/.dbt/profiles.yml'), 'w') as f:
    f.write(f"""tokokita:
  outputs:
    dev:
      type: duckdb
      path: '{PROJECT}/tokokita.duckdb'
      threads: 4
  target: dev
""")
print("✅ Project structure ready")

# %% [3] Generate raw data (simulasi database produksi)
con = duckdb.connect(f'{PROJECT}/tokokita.duckdb')

# --- raw_orders: 200K orders ---
con.sql("""
CREATE TABLE raw_orders AS
WITH rng AS (
    SELECT row_number() OVER () AS rn,
           random() AS r_cust, random() AS r_prod, random() AS r_qty, random() AS r_qty_v,
           random() AS r_date, random() AS r_status, random() AS r_pay,
           random() AS r_plat, random() AS r_plat_v, random() AS r_rating, random() AS r_rating_v
    FROM generate_series(1, 200000)
)
SELECT
    'ORD' || LPAD(CAST(rn AS VARCHAR), 8, '0') AS order_id,
    'USR' || LPAD(CAST(FLOOR(r_cust*10000+1) AS INT)::VARCHAR, 5, '0') AS customer_id,
    'PRD' || LPAD(CAST(FLOOR(r_prod*500+1) AS INT)::VARCHAR, 4, '0') AS product_id,
    CASE WHEN r_qty<.60 THEN 1 WHEN r_qty<.85 THEN 2
         WHEN r_qty<.95 THEN 3 ELSE FLOOR(4+r_qty_v*5)::INT END AS quantity,
    TIMESTAMP '2024-01-01' + INTERVAL (r_date*365) DAY AS order_date,
    CASE WHEN r_status<.75 THEN 'completed' WHEN r_status<.85 THEN 'shipped'
         WHEN r_status<.92 THEN 'processing' WHEN r_status<.97 THEN 'cancelled'
         ELSE 'returned' END AS status,
    CASE WHEN r_pay<.30 THEN 'Bank' WHEN r_pay<.55 THEN 'E-Wallet'
         WHEN r_pay<.70 THEN 'COD' WHEN r_pay<.85 THEN 'CC'
         ELSE 'Paylater' END AS payment_method,
    CASE WHEN r_plat<.05 THEN NULL
         ELSE CASE WHEN r_plat_v<.5 THEN 'mobile' ELSE 'web' END END AS platform,
    CASE WHEN r_rating<.03 THEN -1 WHEN r_rating<.05 THEN NULL
         WHEN r_rating<.08 THEN 0 ELSE FLOOR(1+r_rating_v*5)::INT END AS rating
FROM rng
""")

# --- raw_products: 500 products ---
con.sql("""
CREATE TABLE raw_products AS
WITH rng AS (
    SELECT row_number() OVER () AS rn,
           random() AS r_cat, random() AS r_price, random() AS r_price_v,
           random() AS r_cost, random() AS r_cost_v, random() AS r_disc
    FROM generate_series(1, 500)
)
SELECT
    'PRD' || LPAD(CAST(rn AS VARCHAR), 4, '0') AS product_id,
    CASE WHEN r_cat<.20 THEN 'Fashion' WHEN r_cat<.35 THEN 'Elektronik'
         WHEN r_cat<.50 THEN 'Makanan' WHEN r_cat<.65 THEN 'Kecantikan'
         WHEN r_cat<.80 THEN 'Rumah Tangga' ELSE 'Olahraga' END AS category,
    'Product ' || CAST(rn AS VARCHAR) AS product_name,
    ROUND(CASE WHEN r_price<.3 THEN 25e3+r_price_v*475e3
               WHEN r_price<.5 THEN 100e3+r_price_v*4.9e6
               ELSE 10e3+r_price_v*490e3 END, -2) AS price,
    ROUND(CASE WHEN r_price<.3 THEN 10e3+r_cost_v*200e3
               WHEN r_price<.5 THEN 50e3+r_cost_v*2e6
               ELSE 5e3+r_cost_v*200e3 END, -2) AS cost_price,
    r_disc<.10 AS is_discontinued
FROM rng
""")

# --- raw_customers: 10K customers ---
con.sql("""
CREATE TABLE raw_customers AS
WITH rng AS (
    SELECT row_number() OVER () AS rn,
           random() AS r_city, random() AS r_date, random() AS r_tier, random() AS r_stat
    FROM generate_series(1, 10000)
)
SELECT
    'USR' || LPAD(CAST(rn AS VARCHAR), 5, '0') AS customer_id,
    CASE WHEN r_city<.25 THEN 'Jakarta' WHEN r_city<.40 THEN 'Surabaya'
         WHEN r_city<.55 THEN 'Bandung' WHEN r_city<.65 THEN 'Medan'
         WHEN r_city<.75 THEN 'Semarang' WHEN r_city<.85 THEN 'Yogyakarta'
         ELSE 'Makassar' END AS city,
    TIMESTAMP '2020-01-01' + INTERVAL (r_date*1800) DAY AS registered_date,
    CASE WHEN r_tier<.40 THEN 'regular' WHEN r_tier<.70 THEN 'silver'
         WHEN r_tier<.90 THEN 'gold' ELSE 'platinum' END AS membership_tier,
    CASE WHEN r_stat<.95 THEN 'active' WHEN r_stat<.98 THEN 'inactive'
         ELSE 'ACTIVE' END AS status -- sengaja inkonsisten!
FROM rng
""")
con.close()
print("✅ Raw data: 200K orders, 500 products, 10K customers")

# %% [4] Sources definition
with open(f'{PROJECT}/models/staging/sources.yml', 'w') as f:
    f.write("""version: 2
sources:
  - name: tokokita
    tables:
      - name: raw_orders
        columns:
          - name: order_id
            tests: [unique, not_null]
      - name: raw_products
        columns:
          - name: product_id
            tests: [unique, not_null]
      - name: raw_customers
        columns:
          - name: customer_id
            tests: [unique, not_null]
""")

# %% [5] Staging Models (clean raw data)
# --- stg_orders ---
with open(f'{PROJECT}/models/staging/stg_orders.sql', 'w') as f:
    f.write("""WITH src AS (
    SELECT * FROM {{ source('tokokita', 'raw_orders') }}
)
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    order_date::DATE AS order_date_only,
    status,
    payment_method,
    COALESCE(LOWER(platform), 'unknown') AS platform,
    CASE WHEN rating BETWEEN 1 AND 5 THEN rating END AS rating,
    EXTRACT(YEAR FROM order_date)  AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(DOW FROM order_date) IN (0,6) AS is_weekend
FROM src
WHERE order_id IS NOT NULL
""")

# --- stg_products ---
with open(f'{PROJECT}/models/staging/stg_products.sql', 'w') as f:
    f.write("""WITH src AS (
    SELECT * FROM {{ source('tokokita', 'raw_products') }}
)
SELECT
    product_id, product_name, category, price, cost_price, is_discontinued,
    ROUND((price - cost_price) / NULLIF(price,0) * 100, 1) AS margin_pct,
    CASE WHEN price < 50000 THEN 'Budget'
         WHEN price < 200000 THEN 'Mid'
         WHEN price < 1000000 THEN 'Premium'
         ELSE 'Luxury' END AS price_tier
FROM src
""")

# --- stg_customers ---
with open(f'{PROJECT}/models/staging/stg_customers.sql', 'w') as f:
    f.write("""WITH src AS (
    SELECT * FROM {{ source('tokokita', 'raw_customers') }}
)
SELECT
    customer_id, city, registered_date, membership_tier,
    LOWER(status) AS status,
    DATEDIFF('month', registered_date, CURRENT_DATE) AS tenure_months,
    CASE WHEN DATEDIFF('month', registered_date, CURRENT_DATE) < 6 THEN 'New'
         WHEN DATEDIFF('month', registered_date, CURRENT_DATE) < 24 THEN 'Regular'
         WHEN DATEDIFF('month', registered_date, CURRENT_DATE) < 48 THEN 'Loyal'
         ELSE 'Veteran' END AS tenure_category
FROM src
""")
print("✅ 3 staging models created")

# %% [6] Intermediate Models (join & enrich)
# --- int_order_items ---
with open(f'{PROJECT}/models/intermediate/int_order_items.sql', 'w') as f:
    f.write("""WITH o AS (SELECT * FROM {{ ref('stg_orders') }}),
     p AS (SELECT * FROM {{ ref('stg_products') }})
SELECT
    o.*, p.product_name, p.category, p.price, p.cost_price,
    p.price_tier, p.margin_pct,
    p.price * o.quantity AS revenue,
    p.cost_price * o.quantity AS total_cost,
    (p.price - p.cost_price) * o.quantity AS profit
FROM o
JOIN p ON o.product_id = p.product_id
WHERE p.is_discontinued = false
""")

# --- int_customer_orders ---
with open(f'{PROJECT}/models/intermediate/int_customer_orders.sql', 'w') as f:
    f.write("""WITH oi AS (SELECT * FROM {{ ref('int_order_items') }})
SELECT
    customer_id,
    COUNT(DISTINCT order_id)                                      AS total_orders,
    COUNT(DISTINCT order_id) FILTER (WHERE status='completed')    AS completed,
    COUNT(DISTINCT order_id) FILTER (WHERE status='cancelled')    AS cancelled,
    SUM(CASE WHEN status='completed' THEN revenue ELSE 0 END)    AS total_revenue,
    SUM(CASE WHEN status='completed' THEN profit ELSE 0 END)     AS total_profit,
    AVG(CASE WHEN rating IS NOT NULL THEN rating END)             AS avg_rating,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    COUNT(DISTINCT category) AS unique_categories,
    MODE(payment_method)     AS preferred_payment
FROM oi
GROUP BY customer_id
""")
print("✅ 2 intermediate models created")

# %% [7] Mart Models (consumption-ready)
# --- fct_sales ---
with open(f'{PROJECT}/models/marts/fct_sales.sql', 'w') as f:
    f.write("""SELECT
    order_id, customer_id, product_id, product_name, category, price_tier,
    quantity, revenue, total_cost, profit, margin_pct,
    order_date_only AS sale_date, order_year, order_month, is_weekend,
    status, payment_method, platform, rating
FROM {{ ref('int_order_items') }}
WHERE status = 'completed'
""")

# --- dim_customers ---
with open(f'{PROJECT}/models/marts/dim_customers.sql', 'w') as f:
    f.write("""WITH c AS (SELECT * FROM {{ ref('stg_customers') }}),
     o AS (SELECT * FROM {{ ref('int_customer_orders') }})
SELECT
    c.customer_id, c.city, c.membership_tier, c.status AS account_status,
    c.registered_date, c.tenure_months, c.tenure_category,
    COALESCE(o.total_orders, 0)    AS total_orders,
    COALESCE(o.completed, 0)       AS completed_orders,
    COALESCE(o.total_revenue, 0)   AS lifetime_revenue,
    COALESCE(o.total_profit, 0)    AS lifetime_profit,
    ROUND(COALESCE(o.avg_rating, 0), 2) AS avg_rating,
    o.first_order, o.last_order, o.preferred_payment,
    CASE WHEN COALESCE(o.total_orders,0) = 0 THEN 'Never Purchased'
         WHEN o.total_orders = 1 THEN 'One-time'
         WHEN o.total_orders <= 3 THEN 'Occasional'
         WHEN o.total_orders <= 10 THEN 'Regular'
         ELSE 'Frequent' END AS purchase_segment,
    CASE WHEN COALESCE(o.total_revenue,0) = 0 THEN 'No Revenue'
         WHEN o.total_revenue < 500000 THEN 'Low'
         WHEN o.total_revenue < 2000000 THEN 'Medium'
         WHEN o.total_revenue < 10000000 THEN 'High'
         ELSE 'VIP' END AS value_segment
FROM c LEFT JOIN o ON c.customer_id = o.customer_id
""")

# --- rpt_daily_sales ---
with open(f'{PROJECT}/models/marts/rpt_daily_sales.sql', 'w') as f:
    f.write("""WITH d AS (
    SELECT
        sale_date,
        COUNT(*)                 AS orders,
        SUM(revenue)             AS revenue,
        SUM(profit)              AS profit,
        ROUND(AVG(revenue), 0)   AS aov,
        COUNT(DISTINCT customer_id) AS buyers,
        ROUND(AVG(rating), 2)    AS avg_rating,
        SUM(CASE WHEN platform='mobile' THEN 1 ELSE 0 END) AS mobile_orders
    FROM {{ ref('fct_sales') }}
    GROUP BY sale_date
)
SELECT *,
    AVG(revenue) OVER (ORDER BY sale_date ROWS 6 PRECEDING)  AS ma7,
    AVG(revenue) OVER (ORDER BY sale_date ROWS 29 PRECEDING) AS ma30,
    revenue - LAG(revenue) OVER (ORDER BY sale_date)          AS rev_delta,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY sale_date))
          / NULLIF(LAG(revenue) OVER (ORDER BY sale_date), 0) * 100, 1) AS rev_chg_pct
FROM d ORDER BY sale_date
""")
print("✅ 3 mart models created")

# %% [8] Schema tests
with open(f'{PROJECT}/models/marts/schema.yml', 'w') as f:
    f.write("""version: 2
models:
  - name: fct_sales
    columns:
      - name: order_id
        tests: [not_null]
      - name: revenue
        tests: [not_null]
      - name: rating
        tests:
          - accepted_values:
              values: [1,2,3,4,5]
              config:
                where: "rating IS NOT NULL"
  - name: dim_customers
    columns:
      - name: customer_id
        tests: [unique, not_null]
      - name: value_segment
        tests:
          - accepted_values:
              values: ['No Revenue','Low','Medium','High','VIP']
  - name: rpt_daily_sales
    columns:
      - name: sale_date
        tests: [unique, not_null]
""")

# Custom test
with open(f'{PROJECT}/tests/assert_positive_revenue.sql', 'w') as f:
    f.write("""-- Custom test: revenue harus positif
SELECT * FROM {{ ref('fct_sales') }} WHERE revenue <= 0
""")
print("✅ Tests created")

# %% [9] RUN DBT PIPELINE!
os.chdir(PROJECT)

print("=" * 60)
print("🚀 RUNNING dbt PIPELINE")
print("=" * 60)

!dbt debug 2>&1 | tail -3
print()
!dbt run 2>&1
print()
!dbt test 2>&1

# %% [10] Lihat hasil
con = duckdb.connect(f'{PROJECT}/tokokita.duckdb')

print("\n📊 TABLES IN DATABASE:")
for t in con.sql("SHOW TABLES").fetchall():
    cnt = con.sql(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    print(f"  {t[0]:30s} → {cnt:>10,} rows")

print("\n--- fct_sales (sample) ---")
con.sql("SELECT * FROM fct_sales LIMIT 5").show()

print("\n--- dim_customers segments ---")
con.sql("""
    SELECT value_segment, purchase_segment, COUNT(*) AS n,
           ROUND(AVG(lifetime_revenue),0) AS avg_rev
    FROM dim_customers WHERE total_orders > 0
    GROUP BY value_segment, purchase_segment
    ORDER BY avg_rev DESC LIMIT 12
""").show()

print("\n--- rpt_daily_sales (last 7 days) ---")
con.sql("""
    SELECT sale_date, orders, ROUND(revenue/1e6,1) AS rev_jt,
           ROUND(ma7/1e6,1) AS ma7_jt, buyers, rev_chg_pct
    FROM rpt_daily_sales ORDER BY sale_date DESC LIMIT 7
""").show()

con.close()

# %% [11] Generate docs
!dbt docs generate 2>&1 | tail -3
print("✅ Documentation generated (run `dbt docs serve` locally to view)")

# %% [12] Visualisasi Lineage
print("""
📊 DATA LINEAGE:

raw_orders ──→ stg_orders ───┐
                              ├→ int_order_items ──┬→ fct_sales ──→ rpt_daily_sales
raw_products → stg_products ─┘                     │
                                                   └→ int_customer_orders ─┐
raw_customers→ stg_customers ──────────────────────────────────────────────┼→ dim_customers

dbt otomatis menjalankan model dalam urutan dependency yang benar!
""")
print("✅ Praktikum selesai!")
