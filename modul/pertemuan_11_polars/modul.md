# Pertemuan 11: Polars — High-Performance DataFrame

## Capaian Pembelajaran

1. Membedakan Polars vs Pandas dan kapan menggunakan masing-masing
2. Menggunakan Lazy Evaluation untuk optimasi query otomatis
3. Melakukan data wrangling kompleks (join, group_by, expressions) pada 10 juta baris
4. Menerapkan analisis bisnis: RFM segmentation, cohort analysis

## Kenapa Polars?

| Aspek | Pandas | Polars |
|-------|--------|--------|
| Backend | NumPy (C) | Rust |
| Threading | Single-core | Multi-core otomatis |
| Evaluation | Eager only | Lazy + Eager |
| Memory format | NumPy arrays | Apache Arrow |
| Speed (10M rows) | 1x | 5-50x |
| Memory efficiency | 1x | 2-5x lebih hemat |

## Konsep Kunci: Lazy Evaluation

```
EAGER (Pandas):
  filter → eksekusi
  group  → eksekusi
  sort   → eksekusi
  = 3 kali scan data

LAZY (Polars):
  filter → catat
  group  → catat
  sort   → catat
  collect() → OPTIMASI + eksekusi 1 kali
  = 1 kali scan data (lebih cepat!)
```

Polars otomatis melakukan:
- **Predicate pushdown**: filter diterapkan sedini mungkin
- **Projection pushdown**: hanya kolom yang dibutuhkan yang dibaca
- **Common subexpression elimination**: menghindari komputasi berulang

## Studi Kasus: E-Commerce Indonesia (10 Juta Transaksi)

Analisis bisnis nyata:
- Revenue per kota & kategori
- RFM Customer Segmentation (Recency, Frequency, Monetary)
- Cohort Analysis — retensi pelanggan
- Benchmark Pandas vs Polars vs DuckDB

## Referensi

- Polars User Guide: https://docs.pola.rs/user-guide/
- Polars Expressions: https://docs.pola.rs/user-guide/expressions/
- Apache Arrow: https://arrow.apache.org/
