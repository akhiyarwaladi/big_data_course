# ==============================================================================
# PERTEMUAN 13 — TUGAS: Stream Processing
# ==============================================================================
from collections import defaultdict
import random, time

# TUGAS 1 (Mudah): Trending Hashtag Detection
# ----------------------------------------------
# Gunakan sliding window (5000 events, slide 1000)
# Untuk setiap window:
#   1. Hitung top 5 hashtags
#   2. Bandingkan dengan window sebelumnya
#   3. Jika hashtag BARU masuk top 5, tandai "🔥 TRENDING"
#   4. Jika hashtag turun dari top 5, tandai "📉 DROPPED"
#
# Output: window_id, hashtag, count, status (NEW/STABLE/DROPPED)

# TODO: Implementasi di sini


# TUGAS 2 (Sedang): Fraud Detection Simulator
# -----------------------------------------------
# Simulasikan transaksi e-commerce:
#   - Normal user: 1-3 transaksi per "jam" (per 100 events)
#   - Fraud user: 10+ transaksi per "jam"
#
# Buat:
#   1. Producer yang generate events (5% chance fraud pattern)
#   2. Consumer yang track transaksi per user per window
#   3. Alert jika user melebihi threshold
#   4. Hitung: true positive, false positive rate

# TODO: Implementasi di sini


# TUGAS 3 (Menantang): Deduplication
# ------------------------------------
# Simulasikan masalah "at-least-once delivery":
#   1. Producer mengirim events, 10% adalah duplikat (same event_id)
#   2. Implementasi consumer dengan deduplication (pakai set of seen IDs)
#   3. Bandingkan count: tanpa vs dengan deduplication
#   4. Diskusi: at-least-once vs at-most-once vs exactly-once semantics

# TODO: Implementasi di sini


# TUGAS BONUS: Desain Sistem
# Gambarkan (di kertas/slide) arsitektur streaming untuk salah satu:
#   a) Real-time ride hailing (GoJek) — matching driver + surge pricing
#   b) Live e-commerce flash sale — inventory tracking + fraud detection
#   c) Social media content moderation — real-time toxicity filter
# Jelaskan: producers, topics, consumers, dan windowing strategy
