# Pertemuan 13: Stream Processing & Real-time Analytics

## Capaian Pembelajaran

1. Menjelaskan perbedaan batch vs stream processing dan use case masing-masing
2. Memahami arsitektur event streaming (producer, topic, consumer, offset)
3. Mengimplementasikan windowing (tumbling, sliding) untuk real-time aggregation
4. Membangun anomaly detection pipeline dari IoT sensor data

## Batch vs Stream

| Aspek | Batch | Stream |
|-------|-------|--------|
| Kapan diproses | Terjadwal (jam/hari) | Saat data tiba (real-time) |
| Latensi | Menit ~ jam | Milidetik ~ detik |
| Contoh | Laporan bulanan, ETL | Notifikasi chat, fraud detection |
| Tools | DuckDB, dbt, BigQuery | Kafka, Flink, Redpanda |

## Arsitektur Event Streaming

```
PRODUCER ──→ MESSAGE BROKER ──→ CONSUMER
(app, sensor)   (Kafka)         (analytics, alert)
                  │
              ┌───┴───┐
              │ TOPIC  │  ← Channel untuk jenis event
              │ orders │
              │────────│
              │ TOPIC  │
              │ clicks │
              └────────┘
```

## Windowing

- **Tumbling**: fixed, non-overlapping windows (setiap 5 menit)
- **Sliding**: overlapping windows (window 5 menit, geser 1 menit)
- **Session**: based on activity gap (session berakhir setelah 5 menit idle)

## Catatan

Di praktikum ini kita simulasikan Kafka concepts dengan Python murni (agar bisa jalan di Colab tanpa Docker). Konsep yang dipelajari 100% sama dengan Apache Kafka / Redpanda.

## Referensi

- Apache Kafka: https://kafka.apache.org/
- Confluent Courses (gratis): https://developer.confluent.io/courses/
- "Designing Data-Intensive Applications" Ch. 11 — Martin Kleppmann
