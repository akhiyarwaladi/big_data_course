# ==============================================================================
# PERTEMUAN 14 — TUGAS: Data Lakehouse
# ==============================================================================

# TUGAS 1 (Mudah): Time Travel Queries
# ------------------------------------
# Gunakan lakehouse object dari praktikum.
# 1. Baca snapshot 1 → berapa total rows?
# 2. Baca snapshot 4 → hitung total tax
# 3. Rollback ke snapshot 2 → baca data → berapa rows?
# 4. Bandingkan status distribution antara snapshot 2 dan 3

# TODO: Implementasi di sini


# TUGAS 2 (Sedang): Multi-level Partitioning
# -------------------------------------------
# Re-partition data shipments:
#   Level 1: origin (kota asal)
#   Level 2: service (jenis layanan)
# Struktur: /origin=Jakarta/service=express/data.parquet
#
# Lalu benchmark: query "Jakarta + express" pada
#   a) Single file (non-partitioned)
#   b) Multi-level partitioned
# Berapa kali lebih cepat?

# TODO: Implementasi di sini


# TUGAS 3 (Menantang): Incremental/Upsert Pattern
# ------------------------------------------------
# Simulasi incremental loading:
#   1. Baca latest snapshot
#   2. Generate 500 shipments BARU (new IDs)
#   3. Generate 200 shipments UPDATE (existing IDs, status changed)
#   4. MERGE: insert new + update existing
#   5. Write as new snapshot
#   6. Verify: new rows added, updated rows changed, no duplicates
#
# Ini mensimulasikan Iceberg MERGE INTO

# TODO: Implementasi di sini


# TUGAS BONUS: Medallion Architecture Design
# Gambarkan (slide/kertas) arsitektur Bronze-Silver-Gold untuk
# salah satu perusahaan Indonesia (GoJek, Tokopedia, Traveloka).
# Untuk setiap layer, jelaskan:
#   - Data apa yang disimpan
#   - Format dan partitioning strategy
#   - Transformasi yang dilakukan
#   - Siapa consumer-nya
