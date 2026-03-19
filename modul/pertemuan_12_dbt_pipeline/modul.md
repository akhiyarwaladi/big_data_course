# Pertemuan 12: dbt — Data Pipeline & Transformation Modern

## Capaian Pembelajaran

1. Menjelaskan perbedaan ETL vs ELT dan kenapa ELT lebih modern
2. Membangun data pipeline lengkap menggunakan dbt + DuckDB
3. Menerapkan arsitektur berlapis: staging → intermediate → marts
4. Menulis data quality tests dan membuat dokumentasi otomatis

## ETL vs ELT

```
ETL (lama):  Extract → Transform (di server terpisah) → Load ke warehouse
             ❌ Mahal, lambat, butuh server ETL khusus

ELT (modern): Extract → Load ke warehouse → Transform (DI DALAM warehouse)
             ✅ Murah, cepat, pakai SQL biasa. dbt menangani bagian "T".
```

## Apa Itu dbt?

**dbt (data build tool)** = Framework transformasi data menggunakan SQL + software engineering best practices.

| Fitur | Fungsi |
|-------|--------|
| Models (.sql) | File SQL = satu transformasi |
| `{{ ref() }}` | Referensi antar model (auto-resolve dependency) |
| Tests | Validasi kualitas data otomatis |
| Documentation | Auto-generate docs + lineage graph |
| Materializations | table, view, incremental |

## Arsitektur Berlapis

```
SOURCES (raw data)
  → STAGING (clean, 1-to-1 dengan source)
    → INTERMEDIATE (join, enrich, business logic)
      → MARTS (siap pakai: dashboard, ML, reporting)
```

## Studi Kasus: Pipeline Analytics "TokoKita"

Raw data dari database produksi (orders, products, customers) → dbt transformasi → mart tables siap pakai untuk dashboard dan ML.

## Referensi

- dbt Docs: https://docs.getdbt.com/
- dbt Best Practices: https://docs.getdbt.com/best-practices
- dbt + DuckDB: https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup
