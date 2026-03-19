# Pertemuan 14: Modern Data Lakehouse Architecture

## Capaian Pembelajaran

1. Menjelaskan evolusi Data Warehouse → Data Lake → Data Lakehouse
2. Memahami table formats modern (Apache Iceberg, Delta Lake) dan fiturnya
3. Mengimplementasikan ACID transactions, time travel, schema evolution
4. Membangun data lake dengan partitioning dan medallion architecture

## Tiga Era Penyimpanan Data

| Era | Model | Kelebihan | Kekurangan |
|-----|-------|-----------|-----------|
| **Data Warehouse** (1990-2010) | Structured, SQL | Cepat, rapi | Mahal, hanya structured |
| **Data Lake** (2010-2020) | Semua format, murah | Fleksibel, scalable | Jadi "data swamp", no ACID |
| **Data Lakehouse** (2020+) | Lake + Warehouse features | Murah, ACID, time travel | Butuh table format |

## Table Format = Otak Lakehouse

```
Data Lake biasa:
  /data/orders_jan.parquet   ← Tidak ada proteksi!
  /data/orders_feb.parquet   ← Kalau corrupt? Kalau schema berubah?

Data Lakehouse (+ Iceberg/Delta):
  /metadata/v1.json  ← Snapshot version 1
  /metadata/v2.json  ← Snapshot version 2 (terbaru)
  /data/part-001.parquet
  /data/part-002.parquet

  ✅ ACID, Time Travel, Schema Evolution, Partition Evolution
```

## Medallion Architecture

```
BRONZE (Raw)  →  SILVER (Cleaned)  →  GOLD (Marts)
```

## Studi Kasus: Data Lake Perusahaan Logistik

Data pengiriman partitioned by month, dengan time travel, schema evolution, dan data quality checks.

## Referensi

- Apache Iceberg: https://iceberg.apache.org/
- Delta Lake: https://delta.io/
- Medallion Architecture: https://www.databricks.com/glossary/medallion-architecture
