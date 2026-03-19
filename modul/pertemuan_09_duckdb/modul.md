# Pertemuan 9: DuckDB — Database Analitik Modern

## Capaian Pembelajaran

1. Menjelaskan evolusi Big Data: Hadoop → Spark → Modern Data Stack
2. Menggunakan DuckDB untuk analisis dataset besar (jutaan baris) di Google Colab
3. Menulis SQL analitik: GROUP BY, Window Functions, CTEs
4. Membandingkan performa DuckDB vs Pandas dan memahami Parquet vs CSV

## Kenapa Bukan Hadoop/Spark?

| Aspek | Hadoop/Spark di Colab | DuckDB di Colab |
|-------|----------------------|-----------------|
| Setup | 5-10 menit, sering gagal | `pip install duckdb`, 3 detik |
| Query 5 juta baris | 1-3 menit, sering OOM | < 1 detik |
| Memory | Butuh 4-8 GB RAM | < 500 MB |
| Konsep yang dipelajari | Sama | Sama (SQL, columnar, aggregation) |

Kita mempelajari **konsep yang sama** (SQL analytics, columnar storage, partitioning) tapi dengan tools yang **jauh lebih efisien dan modern**.

## Studi Kasus: GoRide Analytics

Bayangkan Anda adalah data analyst di perusahaan ojek online. Anda punya 10 juta data perjalanan dan diminta menganalisis: kota mana paling menguntungkan? jam sibuk kapan? layanan apa paling populer?

## Struktur Praktikum

| File | Isi |
|------|-----|
| `praktikum.py` | Kode lengkap praktikum (copy ke Colab cell by cell) |
| `tugas.py` | Template tugas untuk dikerjakan mahasiswa |

## Konsep Teori

### 1. Columnar vs Row Storage

```
ROW (CSV/MySQL):                COLUMNAR (Parquet/DuckDB):
┌──────┬──────┬───────┐        ┌──────┬──────┬──────┐
│ nama │ kota │ tarif │        │ nama │ nama │ nama │
│ Andi │ JKT  │ 25000 │        │ Andi │ Budi │ Cici │
│ Budi │ BDG  │ 18000 │        ├──────┴──────┴──────┤
│ Cici │ SBY  │ 22000 │        │ kota │ kota │ kota │
└──────┴──────┴───────┘        │ JKT  │ BDG  │ SBY  │
                                ├──────┴──────┴──────┤
SELECT AVG(tarif)               │tarif │tarif │tarif │
→ Baca SEMUA kolom (lambat)     │25000 │18000 │22000 │
                                └──────┴──────┴──────┘
                                SELECT AVG(tarif)
                                → Baca kolom tarif SAJA (cepat!)
```

### 2. Parquet Format
- **Columnar**: hanya kolom yang di-query yang dibaca
- **Compressed**: 3-10x lebih kecil dari CSV
- **Typed**: tipe data tersimpan (tidak perlu inferensi)
- **Standar industri**: digunakan oleh BigQuery, Snowflake, Spark, DuckDB

### 3. Lazy Evaluation & Query Optimization
DuckDB otomatis mengoptimasi query Anda:
- **Predicate pushdown**: filter diterapkan sedini mungkin
- **Projection pushdown**: hanya kolom yang dibutuhkan yang dibaca
- **Parallel execution**: memanfaatkan semua CPU core

## Referensi

- DuckDB: https://duckdb.org/docs/
- "Why DuckDB": https://duckdb.org/why_duckdb
- Parquet: https://parquet.apache.org/
