# ==============================================================================
# PERTEMUAN 16 — TUGAS: Capstone Project
# ==============================================================================

# ╔═══════════════════════════════════════════════════════════╗
# ║  PROYEK AKHIR — Pilih salah satu:                        ║
# ╠═══════════════════════════════════════════════════════════╣
# ║                                                           ║
# ║  OPSI A: E-Commerce Analytics Platform                    ║
# ║  ─────────────────────────────────────                    ║
# ║  Stack: DuckDB + Polars + dbt + ChromaDB + Streamlit      ║
# ║  Data : Generate atau download dari Kaggle                ║
# ║  Requirements:                                            ║
# ║   □ Raw data ingestion (min 1 juta rows)                  ║
# ║   □ Data transformation (min 3 dbt models)                ║
# ║   □ Analytical queries (min 5 insights)                   ║
# ║   □ RAG chatbot ATAU Streamlit dashboard                  ║
# ║   □ Arsitektur diagram                                    ║
# ║                                                           ║
# ║  OPSI B: IoT Monitoring System                            ║
# ║  ──────────────────────────                               ║
# ║  Stack: Python streams + DuckDB + Streamlit               ║
# ║  Data : Simulated sensor data                             ║
# ║  Requirements:                                            ║
# ║   □ Event producer (min 3 sensor types)                   ║
# ║   □ Windowed aggregation (tumbling + sliding)             ║
# ║   □ Anomaly detection with alerting                       ║
# ║   □ Real-time dashboard (Streamlit / matplotlib)          ║
# ║   □ Historical analysis in DuckDB                         ║
# ║                                                           ║
# ║  OPSI C: Social Media Intelligence                        ║
# ║  ─────────────────────────────────                        ║
# ║  Stack: Python streams + ChromaDB + DuckDB + Streamlit    ║
# ║  Data : Simulated social media events                     ║
# ║  Requirements:                                            ║
# ║   □ Multi-platform event stream                           ║
# ║   □ Trending detection (hashtags / topics)                ║
# ║   □ Sentiment analysis pipeline                           ║
# ║   □ Knowledge base + chatbot (RAG)                        ║
# ║   □ Analytics dashboard                                   ║
# ║                                                           ║
# ╠═══════════════════════════════════════════════════════════╣
# ║                                                           ║
# ║  DELIVERABLES:                                            ║
# ║   1. Architecture diagram (gambar / slide)                ║
# ║   2. Working code (Google Colab notebook / GitHub repo)   ║
# ║   3. Presentation slides (max 10 slides, 10 menit)       ║
# ║   4. Live demo                                            ║
# ║                                                           ║
# ║  GRADING:                                                 ║
# ║   - Architecture & design   : 30%                         ║
# ║   - Working implementation  : 40%                         ║
# ║   - Presentation & demo     : 20%                         ║
# ║   - Creativity & insight    : 10%                         ║
# ║                                                           ║
# ║  DEADLINE: [Sesuai jadwal dosen]                          ║
# ║                                                           ║
# ╚═══════════════════════════════════════════════════════════╝


# TEMPLATE PRESENTASI (10 menit):
# ─────────────────────────────────
# Slide 1: Judul & Anggota Kelompok
# Slide 2: Problem Statement — masalah apa yang diselesaikan?
# Slide 3: Architecture Diagram — komponen apa saja?
# Slide 4: Data Pipeline — dari raw sampai mart
# Slide 5: Key Analytics / Insights — 3 temuan utama
# Slide 6: AI / Streaming Component — RAG atau real-time
# Slide 7: Dashboard Demo (screenshot)
# Slide 8: Live Demo
# Slide 9: Challenges & Learnings
# Slide 10: Q&A


# RUBRIK PENILAIAN DETAIL:
# ────────────────────────
#
# ARSITEKTUR (30%):
#   A : Diagram jelas, komponen lengkap, alur data benar, pertimbangan trade-off
#   B : Diagram ada, sebagian besar komponen ada
#   C : Diagram sederhana, beberapa komponen kurang
#
# IMPLEMENTASI (40%):
#   A : Semua requirements terpenuhi, code bersih, bisa dijalankan
#   B : Sebagian besar requirements, ada minor bugs
#   C : Requirements dasar terpenuhi
#
# PRESENTASI (20%):
#   A : Jelas, terstruktur, demo berjalan lancar
#   B : Cukup jelas, demo sebagian berjalan
#   C : Kurang terstruktur
#
# KREATIVITAS (10%):
#   A : Fitur tambahan yang kreatif, insight yang menarik
#   B : Implementasi standar tapi rapi
#   C : Implementasi minimum
