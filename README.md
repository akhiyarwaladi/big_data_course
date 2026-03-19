# Modern Big Data Engineering — Modul Praktikum

Modul praktikum mata kuliah Big Data untuk pertemuan 9-16. Menggunakan **Modern Data Stack** yang diadopsi universitas terkemuka (UC Berkeley, MIT, Harvard, Stanford).

## Struktur Repository

```
big_data/
├── README.md                          ← File ini
├── modul/
│   ├── 00_silabus.md                  ← Silabus lengkap
│   ├── RESEARCH_modern_bigdata_curriculum.md
│   ├── referensi/                     ← Referensi & link belajar
│   │   └── daftar_referensi.md
│   ├── pertemuan_09_duckdb/
│   │   ├── modul.md                   ← Teori & konsep
│   │   ├── praktikum.py               ← Kode praktikum (copy ke Colab)
│   │   ├── tugas.py                   ← Template tugas mahasiswa
│   │   └── slides.pptx                ← PowerPoint presentasi
│   │
│   ├── pertemuan_10_bigquery/
│   ├── pertemuan_11_polars/
│   ├── pertemuan_12_dbt_pipeline/
│   ├── pertemuan_13_stream_processing/
│   ├── pertemuan_14_data_lakehouse/
│   ├── pertemuan_15_rag_vector_db/
│   └── pertemuan_16_dashboard_capstone/
```

## Jadwal Pertemuan

| # | Topik | Tools | Studi Kasus |
|---|-------|-------|-------------|
| 9 | DuckDB — Database Analitik Modern | DuckDB, Parquet | 10M data GoRide |
| 10 | BigQuery — SQL at Scale di Cloud | BigQuery Free | NYC Taxi + GitHub |
| 11 | Polars — DataFrame Tercepat | Polars | E-Commerce 10M order |
| 12 | dbt — Data Pipeline Modern | dbt + DuckDB | Pipeline TokoKita |
| 13 | Stream Processing | Python | Social Media + IoT |
| 14 | Data Lakehouse | Parquet + Iceberg | Logistik |
| 15 | RAG + Vector Database | ChromaDB | Chatbot E-Commerce |
| 16 | Dashboard + Capstone | Streamlit | End-to-End Project |

## Cara Menggunakan

### Untuk Dosen
1. Buka `slides/` untuk PowerPoint presentasi di kelas
2. Baca `modul.md` di setiap folder pertemuan untuk materi teori
3. Gunakan `tugas.py` sebagai template tugas mahasiswa

### Untuk Mahasiswa
1. Buka Google Colab: https://colab.research.google.com
2. Buat notebook baru
3. Copy code dari `praktikum.py` cell by cell (setiap `# %%` = 1 cell baru)
4. Jalankan setiap cell secara berurutan
5. Kerjakan tugas di `tugas.py`

## Platform (Semua Gratis)

| Tool | Cara Akses |
|------|------------|
| Google Colab | colab.research.google.com |
| DuckDB | `pip install duckdb` |
| Polars | `pip install polars` |
| BigQuery | console.cloud.google.com (1 TB/bulan gratis) |
| dbt | `pip install dbt-duckdb` |
| ChromaDB | `pip install chromadb` |
| Streamlit | `pip install streamlit` |

## Referensi Utama

- Joe Reis — *Fundamentals of Data Engineering* (O'Reilly)
- DataTalksClub — Data Engineering Zoomcamp (GitHub, gratis)
- DuckDB Docs — https://duckdb.org/docs/
- Polars Docs — https://docs.pola.rs/
- dbt Docs — https://docs.getdbt.com/
