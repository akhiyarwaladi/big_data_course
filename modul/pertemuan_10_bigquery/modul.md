# Pertemuan 10: Google BigQuery — SQL at Scale di Cloud

## Capaian Pembelajaran

1. Menggunakan Google BigQuery untuk query dataset berukuran terabyte
2. Memahami arsitektur Cloud Data Warehouse (compute-storage separation)
3. Menulis SQL tingkat lanjut: nested data (UNNEST), approximate functions, partitioning
4. Menganalisis public datasets dengan miliaran baris

## Cloud Data Warehouse vs Database Tradisional

```
DATABASE TRADISIONAL:           CLOUD DATA WAREHOUSE:
┌───────────────────┐          ┌───────────────────────────┐
│ Satu server       │          │ Compute (ribuan CPU)      │
│ ┌──────┐┌──────┐  │          │ Scale up/down otomatis    │
│ │ CPU  ││ Disk │  │          │            ↕              │
│ └──────┘└──────┘  │          │ Storage (petabyte, murah) │
│ Mau cepat?        │          │ Scale independen          │
│ → Beli server     │          │ Bayar per query           │
│   lebih besar     │          │ (bukan per server)        │
└───────────────────┘          └───────────────────────────┘
```

## Studi Kasus

1. **NYC Yellow Taxi Trips** — Puluhan juta perjalanan taxi di New York
2. **GitHub Repositories** — Semua repository publik GitHub (bahasa, commit)

## BigQuery Free Tier

| Item | Gratis |
|------|--------|
| Query | 1 TB per bulan |
| Storage | 10 GB |
| Public Datasets | 200+ dataset, akses gratis |
| Signup | Tanpa kartu kredit (sandbox mode) |

## Tips Hemat BigQuery

```sql
-- ✅ HEMAT: Select kolom spesifik
SELECT pickup_datetime, total_amount FROM ...

-- ❌ BOROS: Select semua kolom
SELECT * FROM ...   -- scan semua kolom = lebih mahal

-- ✅ HEMAT: Filter tanggal (partition pruning)
WHERE pickup_datetime >= '2022-06-01'

-- ✅ Cek biaya sebelum run
-- Di BigQuery Console: lihat "This query will process X GB"
```

## Struktur Praktikum

| File | Isi |
|------|-----|
| `praktikum.py` | Kode BigQuery dari Colab |
| `tugas.py` | Template tugas |

## Referensi

- BigQuery Docs: https://cloud.google.com/bigquery/docs
- Public Datasets: https://cloud.google.com/bigquery/public-data
- BigQuery SQL Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
