# Pertemuan 16: Interactive Dashboard + Capstone Project

## Capaian Pembelajaran

1. Membangun dashboard interaktif menggunakan Streamlit + DuckDB
2. Menghubungkan semua konsep pertemuan 9-15 menjadi end-to-end pipeline
3. Mempresentasikan proyek akhir dengan arsitektur, demo, dan insight

## Streamlit: Dashboard Python Paling Mudah

```python
import streamlit as st
st.title("Hello Dashboard!")
st.metric("Revenue", "Rp 50M", "+25%")
```

Hanya 3 baris = dashboard interaktif! Gratis deploy di Streamlit Cloud atau HuggingFace Spaces.

## Arsitektur End-to-End

```
Data Sources → DuckDB/Polars (process) → dbt (transform) → Lakehouse (store)
                                                                    ↓
                                         Streamlit Dashboard ← DuckDB (query)
                                         RAG Chatbot ← ChromaDB (search)
```

## Capstone Project

Pilih salah satu domain:
1. **E-commerce Analytics Platform** — dari raw data ke dashboard + chatbot
2. **IoT Monitoring Dashboard** — sensor data → anomaly detection → alert dashboard
3. **Social Media Intelligence** — stream analytics → sentiment dashboard + chatbot

### Deliverables
- Arsitektur diagram
- Working code (Google Colab / repo)
- Presentasi 10 menit
- Demo live

### Grading
| Komponen | Bobot |
|----------|-------|
| Arsitektur & design | 30% |
| Working implementation | 40% |
| Presentasi & demo | 20% |
| Kreativitas | 10% |

## Referensi

- Streamlit Docs: https://docs.streamlit.io/
- Streamlit Gallery: https://streamlit.io/gallery
- Streamlit + DuckDB: https://docs.streamlit.io/develop/tutorials/databases/duckdb
