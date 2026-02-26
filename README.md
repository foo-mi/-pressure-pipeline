# 🎵 Julia Wolf Streaming Analytics Pipeline

A lightweight data pipeline that simulates real-time music streaming ingestion
and aggregation — built as a portfolio project demonstrating backend/data
engineering fundamentals.

---

## What It Does

Simulates a Kafka-style event stream of Julia Wolf track plays, processes them
through a stateful aggregation layer, and outputs ranked analytics — the same
pattern used in production ad-tech and music platforms.

```
EventProducer  ──▶  StreamProcessor  ──▶  Report + JSON Snapshot
 (synthetic        (windowed stats,        (leaderboard, deep-
  Kafka topic)      enrichment join)        dive, throughput)
```

**Three simulated "waves"** model real traffic patterns:
- 🚀 New release spike
- 📈 Weekend surge
- 📊 Steady-state baseline

---

## Pipeline Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Event streaming | `EventProducer` emits `StreamEvent` dataclass objects |
| Windowed aggregation | Tumbling 60-second window for throughput calculation |
| Enrichment join | Events enriched with catalog metadata at ingest time |
| Cardinality estimation | Unique user count via in-memory set (HLL-ready pattern) |
| Weighted sampling | Track popularity weights via `random.choices` |
| Batch processing | `process_batch()` for bulk ingest simulation |
| JSON export | Snapshot serialized via `dataclasses.asdict` |

---

## Tech Stack

- **Python 3.11+** — standard library only, zero external dependencies
- Patterns map to production tools: `EventProducer` → Kafka, `StreamProcessor` → Flink/Spark Streaming, JSON output → S3/BigQuery sink

---

## Run It

```bash
python pipeline.py
```

Expected output:
```
Starting Julia Wolf Streaming Analytics Pipeline...
Simulating event ingestion in 3 waves...

  🚀 Wave 1 — New Release Spike   (1,200 events)... done
  📈 Wave 2 — Weekend Surge        (2,500 events)... done
  📊 Wave 3 — Steady State Baseline  (800 events)... done

──────────────────────────────────────────────────────────────
  🎵  Julia Wolf · Streaming Analytics Report
──────────────────────────────────────────────────────────────

Pipeline Stats
  Total events processed : 4,500
  Unique tracks streamed : 8
  Throughput (last 60s)  : X.XX events/sec

Track Leaderboard  (all time)
  🥇 Medicine           ██████████████████████░░░░░░░░  998 streams  complete: 85.2%  top: Apple Music
  🥈 Falling for U      ████████████████████░░░░░░░░░░  812 streams  ...
  ...
```

---

## Extension Ideas

- Swap `EventProducer` for a real Kafka consumer (`confluent-kafka-python`)
- Persist `TrackStats` to Redis for shared state across workers
- Add a Flink-style watermark to handle out-of-order events
- Expose `/metrics` endpoint via FastAPI for Prometheus scraping
- Replace set-based unique count with a true HyperLogLog (Redis `PFADD`)

---

## Why This Project

Ad-tech and streaming platforms process billions of events daily with the same
core patterns shown here: enrichment joins, windowed aggregation, and
cardinality estimation at scale. This project demonstrates familiarity with
those fundamentals in a concrete, runnable form.
