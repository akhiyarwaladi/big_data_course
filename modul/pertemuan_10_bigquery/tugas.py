# ==============================================================================
# PERTEMUAN 10 — TUGAS: Google BigQuery
# Jalankan di BigQuery Console (console.cloud.google.com/bigquery)
# atau dari Colab menggunakan fungsi bq() dari praktikum
# ==============================================================================

# TUGAS 1 (Mudah): Tipping Behavior by Distance
# -----------------------------------------------
# Apakah jarak perjalanan mempengaruhi persentase tip?
# Buat distance buckets: 0-2 miles, 2-5, 5-10, 10-20, 20+
# Hitung avg tip %, jumlah trips per bucket.

"""
SELECT
    CASE
        WHEN trip_distance BETWEEN 0 AND 2 THEN 'a. 0-2 mi'
        WHEN trip_distance BETWEEN 2 AND 5 THEN 'b. 2-5 mi'
        -- TODO: lengkapi bucket
    END AS distance_bucket,
    COUNT(*) AS trips,
    -- TODO: avg tip_amount, avg tip percentage
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
WHERE total_amount BETWEEN 1 AND 500 AND trip_distance > 0
GROUP BY distance_bucket
ORDER BY distance_bucket
"""


# TUGAS 2 (Sedang): Weekday vs Weekend
# -----------------------------------------------
# Bandingkan pola taxi di hari kerja vs akhir pekan:
# avg trips/day, avg fare, avg distance, avg tip%
# Hari apa paling ramai? Hari apa tip paling besar?
#
# Hint: EXTRACT(DAYOFWEEK FROM ...) → 1=Sunday, 7=Saturday

"""
-- Tulis query Anda di sini
"""


# TUGAS 3 (Menantang): Year-over-Year Recovery Analysis
# -----------------------------------------------
# Bandingkan monthly NYC taxi trips: 2020 vs 2021 vs 2022
# Hitung YoY growth % per bulan.
# Pertanyaan: Kapan NYC taxi kembali ke level pre-COVID?
#
# Tabel yang dibutuhkan:
#   tlc_yellow_trips_2020, tlc_yellow_trips_2021, tlc_yellow_trips_2022
#
# Hint: UNION ALL tiga tahun, lalu pivot per bulan

"""
-- Tulis query Anda di sini
"""


# TUGAS BONUS: Buat visualisasi menarik dari BigQuery public dataset
# lain (selain NYC Taxi). Pilih salah satu:
#   - stackoverflow (question trends)
#   - github_repos (language evolution)
#   - covid19_open_data (Indonesia focus)
# Minimal 3 query + 1 chart.
