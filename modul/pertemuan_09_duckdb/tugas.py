# ==============================================================================
# PERTEMUAN 9 — TUGAS PRAKTIKUM
# Kerjakan langsung di Google Colab setelah menjalankan praktikum.py
# ==============================================================================

import duckdb
con = duckdb.connect()

# Load data dari parquet yang sudah dibuat di praktikum
con.sql("CREATE TABLE trips AS SELECT * FROM '/tmp/trips.parquet'")

# ==============================================================================
# TUGAS 1 (Mudah): Analisis Cancellation Rate
# ==============================================================================
# Hitung cancellation rate per kota
# Tampilkan: kota, total_trip, cancelled, cancellation_rate_pct
# Urutkan dari cancellation rate tertinggi
#
# Hint: cancelled = status yang mengandung 'cancelled' atau 'no_driver'

con.sql("""
    SELECT
        kota,
        COUNT(*) AS total_trip,
        -- TODO: hitung jumlah trip yang cancelled (cancelled_user + cancelled_driver + no_driver)
        -- TODO: hitung cancellation_rate_pct
    FROM trips
    GROUP BY kota
    ORDER BY ??? DESC
""").show()


# ==============================================================================
# TUGAS 2 (Sedang): Efektivitas Promo
# ==============================================================================
# Bandingkan trip DENGAN promo vs TANPA promo untuk setiap layanan.
# Tampilkan:
#   layanan, avg_tarif_promo, avg_tarif_no_promo,
#   avg_rating_promo, avg_rating_no_promo
#
# Pertanyaan: Apakah promo meningkatkan rating?
#
# Hint: Gunakan CASE WHEN atau FILTER (WHERE ...)

con.sql("""
    -- Tulis query Anda di sini
""").show()


# ==============================================================================
# TUGAS 3 (Menantang): Peak Revenue Hour per Kota
# ==============================================================================
# Untuk setiap kota, temukan:
#   1. Jam dengan revenue TERTINGGI (peak_revenue_hour)
#   2. Jam dengan trip TERBANYAK (peak_trip_hour)
#   3. Revenue di jam tersebut
#
# Apakah jam paling banyak trip = jam paling banyak revenue?
#
# Hint: Gunakan Window Function (ROW_NUMBER atau RANK)
#       dan CTE bertingkat

con.sql("""
    -- Tulis query Anda di sini
    -- Step 1: Hitung revenue dan trips per kota per jam
    -- Step 2: Rank per kota
    -- Step 3: Ambil rank 1
""").show()


# ==============================================================================
# TUGAS BONUS: Analisis Cohort Mingguan
# ==============================================================================
# Buat analisis mingguan (week-over-week):
#   - minggu (ISO week number)
#   - total_trips, total_revenue, avg_tarif
#   - wow_growth_pct (week-over-week growth percentage)
#
# Hint: Gunakan EXTRACT(WEEK FROM ...) dan LAG()

con.sql("""
    -- Tulis query Anda di sini
""").show()
