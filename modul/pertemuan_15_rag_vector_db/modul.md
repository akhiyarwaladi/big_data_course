# Pertemuan 15: RAG + Vector Database — AI-Powered Data Pipeline

## Capaian Pembelajaran

1. Menjelaskan konvergensi Big Data dan AI (kenapa data engineer perlu tahu AI)
2. Memahami embeddings, vector database, dan similarity search
3. Membangun RAG (Retrieval-Augmented Generation) pipeline
4. Mengintegrasikan data pipeline (pertemuan sebelumnya) dengan AI

## Kenapa Big Data + AI?

```
AI tanpa data = halusinasi
Data tanpa AI = insight manual

Data Pipeline ←→ AI Pipeline  (konvergensi 2025)
```

## RAG: Retrieval-Augmented Generation

```
User Question → RETRIEVE (cari dokumen relevan) → AUGMENT (masukkan ke prompt)
                                                  → GENERATE (LLM jawab)

Tanpa RAG: LLM mengarang (halusinasi)
Dengan RAG: LLM menjawab berdasarkan data Anda
```

## Embeddings

Embedding = representasi numerik dari makna teks (vector 384+ dimensi)

```
"kucing lucu"  → [0.2, 0.8, 0.1, ...]  ← mirip!
"cat cute"     → [0.21, 0.79, 0.12, ...]
"mobil cepat"  → [0.9, 0.1, 0.7, ...]  ← beda jauh
```

Memungkinkan **semantic search** (cari berdasarkan makna, bukan keyword).

## Vector Databases

| DB | Tipe | Cocok untuk |
|----|------|-------------|
| **ChromaDB** | In-process, open source | Belajar, prototyping |
| **Pinecone** | Managed cloud | Production (free tier) |
| **pgvector** | PostgreSQL extension | Sudah pakai Postgres |
| **FAISS** | Library (Meta) | Research |

## Studi Kasus: Chatbot CS "TokoKita"

Knowledge base FAQ + data analytics → semantic search → jawaban akurat.

## Referensi

- ChromaDB: https://docs.trychroma.com/
- Sentence-Transformers: https://www.sbert.net/
- LangChain: https://python.langchain.com/
