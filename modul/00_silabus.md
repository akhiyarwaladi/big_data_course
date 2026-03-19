# SILABUS: Modern Big Data Engineering

## Pertemuan 9-16 (8 Pertemuan Praktikum)

| Item | Detail |
|------|--------|
| **Mata Kuliah** | Big Data |
| **Pertemuan** | 9 - 16 (8x praktikum) |
| **Platform** | Google Colab, Google BigQuery Free Tier, DuckDB |
| **Bahasa** | Python + SQL |
| **Prasyarat** | Dasar Python, Dasar SQL |

## Referensi Kurikulum

Kurikulum disusun berdasarkan pendekatan yang diadopsi universitas terkemuka:
- **UC Berkeley Data 101** - Beralih dari Spark ke BigQuery/SQL-first
- **MIT 6.S079** - Menambahkan RAG, embeddings, LLM ke kurikulum
- **Harvard CS265** - Memasukkan AI ke Big Data Systems
- **Stanford CS246** - Menghentikan kursus Data-Intensive Systems tradisional

## Struktur 8 Pertemuan

| Pertemuan | Topik | Tools | Studi Kasus |
|-----------|-------|-------|-------------|
| **9** | DuckDB: Database Analitik Modern | Colab + DuckDB + Parquet | Analisis 10 Juta Transaksi GoRide |
| **10** | BigQuery: SQL at Scale di Cloud | BigQuery Free Tier | NYC Taxi (1 Miliar Baris) + GitHub |
| **11** | Polars: DataFrame Tercepat | Colab + Polars | E-Commerce Indonesia 10 Juta Order |
| **12** | dbt: Data Pipeline Modern | dbt-core + DuckDB | Pipeline Analytics Toko Online |
| **13** | Stream Processing & Real-time | Python + DuckDB | Social Media Stream + IoT Sensor |
| **14** | Data Lakehouse Architecture | DuckDB + Parquet + Iceberg | Data Lake Perusahaan Logistik |
| **15** | RAG + Vector Database | ChromaDB + Sentence-Transformers | Chatbot CS E-Commerce |
| **16** | Dashboard Interaktif + Capstone | Streamlit + DuckDB | End-to-End Analytics Dashboard |

## Platform & Tools (Semua Gratis)

| Tool | Fungsi | Cara Akses |
|------|--------|------------|
| Google Colab | Notebook Python di cloud | colab.research.google.com |
| DuckDB | Analytical database super cepat | `pip install duckdb` |
| Polars | DataFrame 10-50x lebih cepat dari Pandas | `pip install polars` |
| Google BigQuery | Cloud data warehouse | console.cloud.google.com (1 TB/bulan gratis) |
| dbt-core | Data transformation framework | `pip install dbt-duckdb` |
| ChromaDB | Vector database | `pip install chromadb` |
| Streamlit | Interactive dashboard | `pip install streamlit` |

## Penilaian

| Komponen | Bobot |
|----------|-------|
| Kehadiran & Partisipasi | 10% |
| Tugas Praktikum (8x) | 40% |
| Quiz / UTS | 20% |
| Proyek Akhir (Pertemuan 16) | 30% |

## Buku & Referensi

1. Joe Reis & Matt Housley - *Fundamentals of Data Engineering* (O'Reilly)
2. DataTalksClub - *Data Engineering Zoomcamp* (GitHub, gratis)
3. DuckDB Docs - https://duckdb.org/docs/
4. Polars Docs - https://docs.pola.rs/
5. dbt Docs - https://docs.getdbt.com/
