# ==============================================================================
# PERTEMUAN 15 — RAG + Vector Database
# Studi Kasus: Chatbot Customer Service "TokoKita"
# Platform: Google Colab + ChromaDB + Sentence-Transformers
# ==============================================================================

# %% [1] Setup
!pip install chromadb sentence-transformers duckdb polars -q

import chromadb
from sentence_transformers import SentenceTransformer
import duckdb, polars as pl, numpy as np
import time, json

print("✅ Ready!")

# %% [2] Memahami Embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dimensions, gratis

texts = [
    "Harga iPhone 15 Pro Max terbaru",
    "Berapa biaya Apple smartphone flagship",
    "Cara memasak nasi goreng yang enak",
    "Resep masakan Indonesia pedas",
    "Jadwal kereta api Jakarta Surabaya",
    "Transportasi kereta dari ibukota ke Jawa Timur",
]

embeddings = model.encode(texts)
print(f"Texts: {len(texts)}, Dimensions: {embeddings.shape[1]}")

# Cosine similarity matrix
from numpy.linalg import norm
def cos_sim(a, b):
    return np.dot(a, b) / (norm(a) * norm(b))

print("\n📊 Similarity Matrix:")
labels = [f"T{i+1}" for i in range(len(texts))]
print(f"{'':>6}", '  '.join(labels))
for i in range(len(texts)):
    sims = [f"{cos_sim(embeddings[i], embeddings[j]):.2f}" for j in range(len(texts))]
    short = texts[i][:35]
    print(f"T{i+1} {'  '.join(sims)}  {short}")

print("\nT1≈T2 (iPhone/smartphone), T3≈T4 (masakan), T5≈T6 (kereta)")

# %% [3] Build Knowledge Base
client = chromadb.Client()
collection = client.get_or_create_collection("tokokita_kb")

knowledge = [
    # Return policy
    {"id": "ret1", "text": "Pengembalian barang TokoKita: 7 hari setelah terima, kondisi asli, refund 3-5 hari kerja.", "cat": "kebijakan", "topic": "return"},
    {"id": "ret2", "text": "Garansi produk elektronik 1 tahun. Kerusakan pemakaian normal ditanggung. Kerusakan air/jatuh tidak ditanggung.", "cat": "kebijakan", "topic": "garansi"},
    # Shipping
    {"id": "ship1", "text": "Jenis pengiriman: Regular (3-5 hari, Rp 8.000), Express (1-2 hari, Rp 15.000), Same Day (hari itu, Rp 25.000, kota besar), Cargo (>20kg, Rp 50.000).", "cat": "pengiriman", "topic": "layanan"},
    {"id": "ship2", "text": "Gratis ongkir untuk pembelian min Rp 100.000 layanan Regular. Antar pulau: subsidi max Rp 20.000.", "cat": "pengiriman", "topic": "gratis_ongkir"},
    {"id": "ship3", "text": "Estimasi: dalam kota 1-2 hari, dalam pulau 2-4 hari, antar pulau 4-7 hari, terpencil 7-14 hari.", "cat": "pengiriman", "topic": "estimasi"},
    # Payment
    {"id": "pay1", "text": "Metode bayar: Transfer Bank (BCA/Mandiri/BNI/BRI), E-Wallet (GoPay/OVO/Dana), VA, Kartu Kredit, COD.", "cat": "pembayaran", "topic": "metode"},
    {"id": "pay2", "text": "Cicilan 0% min Rp 500.000 dengan CC BCA/Mandiri/BNI (3/6/12 bulan). PayLater via GoPay Later & Kredivo max Rp 10 juta.", "cat": "pembayaran", "topic": "cicilan"},
    # Products
    {"id": "prod1", "text": "Kategori terlaris 2024: Fashion 35%, Elektronik 20%, F&B 18%, Kecantikan 15%, Rumah Tangga 12%. Terlaris: Samsung Galaxy S24.", "cat": "produk", "topic": "terlaris"},
    {"id": "prod2", "text": "Official Store menjamin keaslian 100%. Produk palsu: refund 2x lipat. 5.000+ brand terdaftar.", "cat": "produk", "topic": "official"},
    # Promo
    {"id": "promo1", "text": "TokoKita Rewards: 1 poin per Rp 1.000. Level: Regular/Silver/Gold/Platinum. Gold+ diskon 10% tiap Jumat.", "cat": "promo", "topic": "loyalty"},
    {"id": "promo2", "text": "Flash sale tanggal kembar (1.1, 2.2, dst) dan gajian (25, 28). Diskon 90%. Voucher BARUGABUNG cashback 50%.", "cat": "promo", "topic": "flash_sale"},
    # CS
    {"id": "cs1", "text": "CS 24/7: Live Chat (<2 mnt), Email cs@tokokita.id (<24 jam), WA 0812-1234-5678 (<30 mnt), Telp 021-12345.", "cat": "cs", "topic": "kontak"},
    {"id": "cs2", "text": "Komplain barang rusak: foto barang+kemasan, ajukan 2x24 jam, review 1 hari kerja, refund atau kirim ulang.", "cat": "cs", "topic": "komplain"},
    # Data insights
    {"id": "data1", "text": "Q4 2024: Revenue Rp 50 miliar (+25% QoQ). Kota terbesar: Jakarta 30%, Surabaya 15%, Bandung 12%. Peak: 19-22 WIB.", "cat": "data", "topic": "revenue"},
    {"id": "data2", "text": "Q4 2024: 500K active users, 40% repeat buyers. AOV Rp 185.000. CSAT 4.5/5. Top payment: E-Wallet 35%.", "cat": "data", "topic": "customers"},
]

collection.add(
    documents=[k['text'] for k in knowledge],
    ids=[k['id'] for k in knowledge],
    metadatas=[{'cat': k['cat'], 'topic': k['topic']} for k in knowledge],
)
print(f"✅ Knowledge base: {collection.count()} documents")

# %% [4] Semantic Search
print("🔍 SEMANTIC SEARCH")
print("=" * 55)

queries = [
    "cara mengembalikan barang yang sudah dibeli",
    "berapa lama pengiriman ke Surabaya?",
    "bisa bayar pakai GoPay?",
    "ada diskon apa bulan ini?",
    "barang rusak saat diterima",
    "berapa revenue perusahaan?",
    "minimal belanja gratis ongkir?",
]

for q in queries:
    r = collection.query(query_texts=[q], n_results=2)
    doc = r['documents'][0][0][:80]
    meta = r['metadatas'][0][0]
    dist = r['distances'][0][0]
    rel = max(0, (2-dist)/2*100)
    print(f"\n❓ {q}")
    print(f"   [{meta['cat']}/{meta['topic']}] {rel:.0f}% → {doc}...")

# %% [5] Filtered Search
print("\n\n🔍 FILTERED SEARCH (by category)")
r = collection.query(
    query_texts=["harga kirim barang"],
    n_results=3,
    where={"cat": "pengiriman"},
)
for doc, meta in zip(r['documents'][0], r['metadatas'][0]):
    print(f"  [{meta['topic']}] {doc[:100]}...")

# %% [6] RAG Pipeline
class TokoKitaBot:
    def __init__(self, collection):
        self.col = collection
        self.history = []

    def retrieve(self, query, n=3, cat=None):
        where = {"cat": cat} if cat else None
        r = self.col.query(query_texts=[query], n_results=n, where=where)
        docs = []
        for doc, meta, dist in zip(r['documents'][0], r['metadatas'][0], r['distances'][0]):
            docs.append({'text': doc, 'cat': meta['cat'], 'topic': meta['topic'],
                         'relevance': max(0, (2-dist)/2*100)})
        return docs

    def build_prompt(self, query, docs):
        ctx = "\n\n".join(f"[{d['cat']}/{d['topic']}]: {d['text']}" for d in docs)
        return f"""Kamu asisten TokoKita. Jawab berdasarkan HANYA konteks berikut.
Jika info tidak ada, katakan tidak tersedia.

KONTEKS:
{ctx}

PERTANYAAN: {query}
JAWABAN:"""

    def respond(self, query, docs):
        if not docs or docs[0]['relevance'] < 30:
            return "Maaf, saya tidak menemukan info tersebut. Hubungi CS: 021-12345."
        ans = f"Berdasarkan data kami:\n\n{docs[0]['text']}"
        if len(docs)>1 and docs[1]['relevance']>50:
            ans += f"\n\nInfo tambahan: {docs[1]['text']}"
        return ans

    def chat(self, query, cat=None, verbose=False):
        t0 = time.time()
        docs = self.retrieve(query, cat=cat)
        prompt = self.build_prompt(query, docs)
        answer = self.respond(query, docs)
        ms = (time.time()-t0)*1000

        self.history.append({'q': query, 'a': answer, 'ms': ms,
                             'rel': docs[0]['relevance'] if docs else 0})
        if verbose:
            print(f"\n{'='*55}")
            print(f"❓ {query}")
            for i, d in enumerate(docs):
                print(f"   {i+1}. [{d['cat']}/{d['topic']}] {d['relevance']:.0f}%")
            print(f"\n🤖 {answer}")
            print(f"⚡ {ms:.0f}ms")
        else:
            print(f"\n❓ {query}")
            print(f"🤖 {answer[:150]}...")
            print(f"   ({ms:.0f}ms | {len(docs)} docs | {docs[0]['relevance']:.0f}%)")
        return answer

bot = TokoKitaBot(collection)
print("✅ TokoKita Bot ready!")

# %% [7] Demo Chatbot
print("💬 CHATBOT DEMO")
print("=" * 55)

for q in [
    "Mau kembalikan barang yang dibeli 3 hari lalu",
    "Kirim ke Surabaya berapa lama?",
    "Bisa cicilan 0%?",
    "Kapan flash sale berikutnya?",
    "Barang rusak, mau klaim garansi",
    "Berapa revenue Q4?",
    "Cara daftar jadi seller",  # not in KB!
]:
    bot.chat(q)

# %% [8] Verbose — Lihat detail RAG
bot.chat("komplain barang elektronik rusak mau garansi", verbose=True)

# %% [9] Show augmented prompt (untuk edukasi)
print("\n📝 CONTOH PROMPT YANG DIKIRIM KE LLM:")
print("=" * 55)
docs = bot.retrieve("gratis ongkir minimal belanja berapa?")
print(bot.build_prompt("gratis ongkir minimal belanja berapa?", docs))
print("\nDi production, prompt ini dikirim ke OpenAI/Claude/Gemini API")

# %% [10] Integrasi: Data Pipeline → AI Knowledge Base
print("\n🔗 INTEGRASI: Data Pipeline → Vector DB")
print("=" * 55)

con = duckdb.connect()
con.sql("""
CREATE TABLE daily_insights AS
SELECT
    CAST(DATE '2025-01-01' + INTERVAL (day-1) DAY AS DATE) AS tanggal,
    FLOOR(1000+random()*500)::INT AS orders,
    ROUND((100+random()*50)*1e6) AS revenue,
    ROUND(3.5+random()*1.5, 2) AS avg_rating,
    CASE WHEN random()<.3 THEN 'Fashion' WHEN random()<.6 THEN 'Elektronik'
         ELSE 'F&B' END AS top_cat
FROM generate_series(1, 31) AS t(day)
""")

insights = con.sql("""
    SELECT tanggal,
           'Tanggal ' || tanggal || ': ' || orders || ' pesanan, revenue Rp '
           || CAST(ROUND(revenue/1e6) AS INT) || ' juta, rating ' || avg_rating
           || '/5, kategori terlaris: ' || top_cat || '.' AS text
    FROM daily_insights ORDER BY tanggal
""").fetchall()

for i, (dt, txt) in enumerate(insights):
    collection.add(documents=[txt], ids=[f"daily_{i+1:02d}"],
                   metadatas=[{"cat": "data", "topic": f"daily_{dt}"}])

print(f"✅ Added {len(insights)} daily insights → total: {collection.count()} docs")

# Test
bot.chat("performa penjualan tanggal 15 Januari?", verbose=True)
con.close()

# %% [11] Full Architecture Diagram
print("""
🏗️ ARSITEKTUR: DATA PIPELINE → AI CHATBOT

  Sources          Pipeline           AI
  ────────         ────────           ──
  Database  ──→  DuckDB/Polars  ──→  Embedding Model
  API       ──→  dbt transform  ──→  Vector DB (ChromaDB)
  Streaming ──→  Lakehouse      ──→  RAG Pipeline
                    │                    │
                    ▼                    ▼
                 Dashboard           Chatbot
                 (Streamlit)         (User-facing)

  Pertemuan 9-14 ─────────────→  Pertemuan 15
  Data Engineering                AI Engineering
""")
print("✅ Praktikum selesai!")
