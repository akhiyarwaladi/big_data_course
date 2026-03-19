# ==============================================================================
# PERTEMUAN 15 — TUGAS: RAG + Vector Database
# ==============================================================================

# TUGAS 1 (Mudah): Expand Knowledge Base
# ----------------------------------------
# Tambahkan 5+ dokumen tentang kebijakan seller/merchant TokoKita:
#   - Cara daftar jadi seller
#   - Biaya komisi per kategori
#   - Aturan listing produk
#   - Proses verifikasi
#   - Penalti pelanggaran
# Lalu test dengan 3 pertanyaan yang relevan.

# new_docs = [
#     {"id": "seller1", "text": "...", "cat": "seller", "topic": "daftar"},
#     ...
# ]
# collection.add(documents=..., ids=..., metadatas=...)
# bot.chat("cara jadi seller di TokoKita")


# TUGAS 2 (Sedang): Retrieval Quality Evaluation
# -------------------------------------------------
# Buat evaluation framework:
# 1. Definisikan 10 test cases: (pertanyaan, expected_category)
# 2. Jalankan retrieval untuk setiap pertanyaan
# 3. Cek apakah top-1 result match expected category
# 4. Hitung accuracy, precision, avg relevance score

# test_cases = [
#     ("cara retur barang", "kebijakan"),
#     ("ongkir ke Bali", "pengiriman"),
#     ("bisa pakai dana?", "pembayaran"),
#     # ... 7 more
# ]
#
# correct = 0
# for q, expected_cat in test_cases:
#     results = collection.query(query_texts=[q], n_results=1)
#     actual_cat = results['metadatas'][0][0]['cat']
#     match = actual_cat == expected_cat
#     correct += match
#     print(f"{'✅' if match else '❌'} Q: {q} | expected: {expected_cat} | got: {actual_cat}")
#
# print(f"\nAccuracy: {correct}/{len(test_cases)} = {correct/len(test_cases)*100:.0f}%")


# TUGAS 3 (Menantang): Multi-turn Conversation
# -----------------------------------------------
# Implementasi context-aware chatbot:
#   User: "Berapa ongkir ke Surabaya?"
#   Bot: "Regular Rp 8000, Express Rp 15000..."
#   User: "Kalau same day?"  ← harus tahu konteks "ke Surabaya"
#
# Approach: gabungkan query saat ini + context dari N chat terakhir
# lalu retrieve berdasarkan gabungan tersebut.

# class ContextAwareBot(TokoKitaBot):
#     def chat_with_context(self, query, n_context=2):
#         # Ambil N chat terakhir dari history
#         context = ' '.join(h['q'] for h in self.history[-n_context:])
#         enriched_query = f"{context} {query}"
#         return self.chat(enriched_query)


# TUGAS BONUS: LLM Integration (opsional, butuh API key)
# Jika Anda punya API key OpenAI/Gemini/Claude:
# Ganti method respond() untuk memanggil LLM API
# dengan augmented prompt yang sudah dibuat.
# Bandingkan kualitas jawaban rule-based vs LLM.
