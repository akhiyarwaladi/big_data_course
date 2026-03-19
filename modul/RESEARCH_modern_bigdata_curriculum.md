# Research Report: Modern Big Data Curriculum 2025-2026
## Mengapa Hadoop/Spark Tidak Lagi Relevan & Apa Penggantinya

---

## Executive Summary

Universitas-universitas top dunia telah secara signifikan mengubah kurikulum Big Data mereka. UC Berkeley secara eksplisit berpindah dari Spark ke BigQuery/Snowflake. MIT menambahkan RAG dan LLM ke kurikulumnya. Harvard memasukkan AI ke dalam kursus Big Data. Stanford bahkan menghentikan kursus CS245 tentang Data-Intensive Systems.

Tren utama: **SQL-first, Cloud-native, Python-friendly, AI-integrated**.

---

## 1. Apa yang Diajarkan Universitas Top Saat Ini

### UC Berkeley - Data 101: Data Engineering
- **Pivot signifikan**: Berpindah dari Spark/RDD ke **BigQuery dan Snowflake**
- Alasan: Industri (Trifacta, Databricks) sendiri lebih banyak menggunakan SQL
- Paper ACM: "Piloting Data Engineering at Berkeley" mendokumentasikan keputusan ini
- Fokus: SQL sebagai bahasa transformasi utama

### MIT - 6.S079: Software Systems for Data Science
- Tetap mencakup Spark, tapi menambahkan **embeddings, RAG, dan LLM**
- 6 hands-on labs + proyek semester
- MIT xPRO: Menggunakan **BigQuery, SQLite, streaming databases**

### Harvard - CS265: Big Data & AI Systems (Spring 2026)
- Sekarang secara eksplisit mencakup **NoSQL, LLM, RAG, dan Image AI**
- Proyek termasuk membangun LLM inference systems

### Stanford - CS246: Mining Massive Data Sets
- Masih menggunakan Spark, tapi fokus pada algoritma bukan infrastruktur
- **CS245 (Principles of Data-Intensive Systems) dihentikan Fall 2024**

### Carnegie Mellon - 15-445/645: Database Systems
- Menambahkan **vector indexes** untuk AI workloads
- Fokus pada arsitektur modern, query optimization

---

## 2. Tools Modern Pengganti Hadoop/Spark

### Untuk Query & Analytics
| Tool | Pengganti | Free Tier |
|------|-----------|-----------|
| **DuckDB** | Hive, SparkSQL lokal | 100% gratis, open source |
| **Google BigQuery** | Hadoop cluster | 1 TB query/bulan gratis |
| **Snowflake** | Hadoop cluster | 120-hari trial, $400 credits |

### Untuk Data Processing
| Tool | Pengganti | Kelebihan |
|------|-----------|-----------|
| **Polars** | Pandas (untuk big data) | 5-50x lebih cepat, multi-threaded |
| **DuckDB** | PySpark | Zero setup, super cepat di Colab |

### Untuk ETL/Transformation
| Tool | Pengganti | Kelebihan |
|------|-----------|-----------|
| **dbt** | Custom Spark ETL | SQL-based, version control, testing |
| **Dagster/Airflow** | Oozie | Modern UI, Python-native |

### Untuk Streaming
| Tool | Pengganti | Kelebihan |
|------|-----------|-----------|
| **Apache Kafka** | Spark Streaming | Industry standard |
| **Redpanda** | Kafka (untuk belajar) | No JVM, single binary |

### Untuk Storage
| Tool | Pengganti | Kelebihan |
|------|-----------|-----------|
| **Apache Iceberg** | HDFS + Hive | ACID, time travel, open format |
| **Parquet** | CSV/text files | Columnar, 5-10x lebih kecil |

### Untuk AI/ML
| Tool | Baru | Kelebihan |
|------|------|-----------|
| **ChromaDB/Pinecone** | Vector database | Semantic search |
| **RAG Pipeline** | LLM + retrieval | AI di atas data internal |

---

## 3. Platform Gratis untuk Praktikum

### Tier 1: Selalu Gratis
1. **Google Colab** - Jupyter notebook gratis, GPU/TPU
2. **DuckDB** - 100% open source, install `pip install duckdb`
3. **Google BigQuery** - 1 TB query + 10 GB storage/bulan
4. **Kaggle Notebooks** - Compute gratis + dataset
5. **ChromaDB** - Vector database open source

### Tier 2: Educational Credits
1. **Databricks Free Edition** - Gratis untuk students/educators
2. **Snowflake for Academia** - Akses gratis + kurikulum
3. **Google Cloud Education Credits** - $50/student + $100/faculty
4. **GitHub Codespaces** - 180 core-hours/bulan (GitHub Education)

---

## 4. Mengapa Hadoop/Spark Kurang Relevan

### Fakta Industri
- Hortonworks-Cloudera merger menunjukkan declining market
- Databricks sendiri melihat lebih banyak adopsi SQL daripada RDD/DataFrame
- Cloud-native solutions (BigQuery, Snowflake) menggantikan on-premise Hadoop
- DuckDB bisa memproses ratusan juta baris di laptop tanpa cluster

### Masalah di Kelas
- PySpark di Google Colab: lambat startup, sering OOM, error JVM
- Setup cluster Hadoop: memakan waktu berminggu-minggu, bukan belajar konsep
- Mahasiswa frustrasi dengan infrastruktur, bukan belajar data engineering

### Yang Tetap Relevan dari Spark
- Konsep distributed computing (diajarkan secara teori)
- Spark tetap digunakan di industri untuk truly massive scale
- Databricks (berbasis Spark) masih relevan, tapi dengan UI modern

---

## 5. Rekomendasi Kurikulum 7 Pertemuan

| No | Topik | Tools | Studi Kasus |
|----|-------|-------|-------------|
| 1 | Modern Big Data & DuckDB | Colab + DuckDB | Analisis Ojek Online |
| 2 | SQL at Scale - BigQuery | BigQuery Free | NYC Taxi + GitHub |
| 3 | High-Performance Processing | Polars | E-commerce Indonesia |
| 4 | Data Pipeline - dbt | dbt + DuckDB | Pipeline Toko Online |
| 5 | Stream Processing | Python simulasi | Social Media + IoT |
| 6 | Data Lakehouse | Parquet + DuckDB | Logistik |
| 7 | AI Data Pipeline - RAG | ChromaDB | Chatbot Pintar |

---

## Sumber Riset

1. UC Berkeley Data 101 - https://data101.org/
2. ACM Paper "Piloting Data Engineering at Berkeley" - https://dl.acm.org/doi/fullHtml/10.1145/3531072.3535324
3. MIT 6.S079 - https://dsg.csail.mit.edu/6.S079/
4. Harvard CS265 Spring 2026 - http://daslab.seas.harvard.edu/classes/cs265/
5. Stanford CS246 - https://cs246.stanford.edu/
6. CMU 15-445/645 - https://15445.courses.cs.cmu.edu/fall2025/
7. DataTalksClub Data Engineering Zoomcamp - https://github.com/DataTalksClub/data-engineering-zoomcamp
8. Databricks University Alliance - https://www.databricks.com/resources/teach
9. DuckDB Documentation - https://duckdb.org/
10. Polars Documentation - https://docs.pola.rs/
