"""
Julia Wolf Streaming Analytics Pipeline
----------------------------------------
Simulates a real-time ingestion + aggregation pipeline over a music catalog.
Demonstrates: event streaming, windowed aggregation, enrichment joins,
and a lightweight in-memory store — patterns common in ad-tech data systems.

Run: python pipeline.py
"""

import random
import time
import json
import hashlib
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Optional


# ---------------------------------------------------------------------------
# 1. CATALOG — Julia Wolf track data
# ---------------------------------------------------------------------------

CATALOG = [
    {"track_id": "jw-001", "title": "In My Room",              "album": "In My Room (Single)",  "duration_s": 183, "release_year": 2024},
    {"track_id": "jw-002", "title": "Last Summer",             "album": "Last Summer (Single)", "duration_s": 172, "release_year": 2024},
    {"track_id": "jw-003", "title": "Kill You Off",            "album": "PRESSURE",             "duration_s": 148, "release_year": 2025},
    {"track_id": "jw-004", "title": "Pearl",                   "album": "PRESSURE",             "duration_s": 141, "release_year": 2025},
    {"track_id": "jw-005", "title": "Loser",                   "album": "PRESSURE",             "duration_s": 167, "release_year": 2025},
    {"track_id": "jw-006", "title": "Fingernails",             "album": "PRESSURE",             "duration_s": 144, "release_year": 2025},
    {"track_id": "jw-007", "title": "Limewire",                "album": "PRESSURE",             "duration_s": 158, "release_year": 2025},
    {"track_id": "jw-008", "title": "Jennifer's Body",         "album": "PRESSURE",             "duration_s": 153, "release_year": 2025},
    {"track_id": "jw-009", "title": "Sunshine State",          "album": "PRESSURE",             "duration_s": 176, "release_year": 2025},
    {"track_id": "jw-010", "title": "You've Lost A Lot of Blood", "album": "PRESSURE",          "duration_s": 182, "release_year": 2025},
]

PLATFORMS   = ["Apple Music", "Spotify", "YouTube Music", "Amazon Music", "Tidal"]
GEO_REGIONS = ["US-West", "US-East", "US-Central", "EU-West", "APAC", "LATAM"]
DEVICES     = ["iPhone", "iPad", "Mac", "Android", "Smart TV", "Web"]

# Weighted popularity — "In My Room" is the breakout hit (17 weeks on charts)
TRACK_WEIGHTS = [0.24, 0.10, 0.13, 0.11, 0.12, 0.09, 0.08, 0.06, 0.04, 0.03]


# ---------------------------------------------------------------------------
# 2. DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class StreamEvent:
    event_id:    str
    track_id:    str
    user_id:     str
    platform:    str
    region:      str
    device:      str
    timestamp:   float          # unix epoch
    listened_s:  int            # seconds actually played
    completed:   bool           # listened >= 70% of track

@dataclass
class TrackStats:
    track_id:     str
    title:        str
    total_streams:  int   = 0
    unique_users:   int   = 0     # approximated via HyperLogLog-style set
    completion_pct: float = 0.0
    avg_listen_s:   float = 0.0
    top_platform:   str   = ""
    top_region:     str   = ""
    _user_set:      set   = field(default_factory=set, repr=False)
    _listen_total:  int   = field(default=0, repr=False)
    _completions:   int   = field(default=0, repr=False)
    _platform_counts: dict = field(default_factory=dict, repr=False)
    _region_counts:   dict = field(default_factory=dict, repr=False)

    def ingest(self, event: StreamEvent):
        self.total_streams  += 1
        self._user_set.add(event.user_id)
        self.unique_users    = len(self._user_set)
        self._listen_total  += event.listened_s
        self.avg_listen_s    = self._listen_total / self.total_streams
        if event.completed:
            self._completions += 1
        self.completion_pct  = round(self._completions / self.total_streams * 100, 1)
        self._platform_counts[event.platform] = self._platform_counts.get(event.platform, 0) + 1
        self._region_counts[event.region]     = self._region_counts.get(event.region, 0) + 1
        self.top_platform    = max(self._platform_counts, key=self._platform_counts.get)
        self.top_region      = max(self._region_counts,   key=self._region_counts.get)


# ---------------------------------------------------------------------------
# 3. EVENT GENERATOR  (simulates a Kafka-like producer)
# ---------------------------------------------------------------------------

class EventProducer:
    """Generates synthetic streaming events for Julia Wolf tracks."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self._user_pool = [f"user_{i:05d}" for i in range(1, 5001)]  # 5K simulated users

    def emit(self, ts_offset_s: float = 0.0) -> StreamEvent:
        track   = random.choices(CATALOG, weights=TRACK_WEIGHTS, k=1)[0]
        listened = int(random.betavariate(5, 2) * track["duration_s"])
        listened = max(10, min(listened, track["duration_s"]))
        completed = listened >= 0.70 * track["duration_s"]
        raw_id = f"{track['track_id']}-{time.time()}-{random.random()}"
        return StreamEvent(
            event_id   = hashlib.md5(raw_id.encode()).hexdigest()[:12],
            track_id   = track["track_id"],
            user_id    = random.choice(self._user_pool),
            platform   = random.choice(PLATFORMS),
            region     = random.choices(
                GEO_REGIONS,
                weights=[0.30, 0.25, 0.15, 0.15, 0.10, 0.05],
                k=1
            )[0],
            device     = random.choices(
                DEVICES,
                weights=[0.38, 0.12, 0.18, 0.22, 0.05, 0.05],
                k=1
            )[0],
            timestamp  = time.time() + ts_offset_s,
            listened_s = listened,
            completed  = completed,
        )

    def batch(self, n: int) -> list[StreamEvent]:
        return [self.emit(i * 0.1) for i in range(n)]


# ---------------------------------------------------------------------------
# 4. STREAM PROCESSOR  (windowed aggregation — tumbling 60-second windows)
# ---------------------------------------------------------------------------

class StreamProcessor:
    """
    Processes events and maintains:
      - per-track aggregate stats
      - a sliding window for recent throughput
      - a simple anomaly flag (spike > 3× rolling avg)
    """

    WINDOW_SIZE = 60  # seconds

    def __init__(self):
        self._catalog_map = {t["track_id"]: t for t in CATALOG}
        self.stats: dict[str, TrackStats] = {
            t["track_id"]: TrackStats(t["track_id"], t["title"])
            for t in CATALOG
        }
        self._window: deque[float] = deque()  # timestamps for throughput calc
        self._total_processed = 0
        self._anomalies: list[str] = []

    def process(self, event: StreamEvent):
        # Enrich: guard against unknown track
        if event.track_id not in self.stats:
            return

        self.stats[event.track_id].ingest(event)
        self._total_processed += 1

        # Sliding window throughput
        now = time.time()
        self._window.append(now)
        cutoff = now - self.WINDOW_SIZE
        while self._window and self._window[0] < cutoff:
            self._window.popleft()

    def throughput(self) -> float:
        """Events per second over the last WINDOW_SIZE seconds."""
        return len(self._window) / self.WINDOW_SIZE

    def process_batch(self, events: list[StreamEvent]):
        for e in events:
            self.process(e)

    def top_tracks(self, n: int = 5) -> list[TrackStats]:
        return sorted(
            self.stats.values(),
            key=lambda s: s.total_streams,
            reverse=True
        )[:n]

    def summary(self) -> dict:
        top = self.top_tracks(3)
        return {
            "total_events_processed": self._total_processed,
            "unique_tracks_streamed":  sum(1 for s in self.stats.values() if s.total_streams > 0),
            "throughput_eps":          round(self.throughput(), 2),
            "top_3_tracks":            [{"title": s.title, "streams": s.total_streams} for s in top],
        }


# ---------------------------------------------------------------------------
# 5. REPORTING
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[36m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"

def bar(value: float, max_val: float, width: int = 30) -> str:
    filled = int(round(value / max_val * width)) if max_val else 0
    return "█" * filled + "░" * (width - filled)

def print_report(processor: StreamProcessor):
    print(f"\n{BOLD}{CYAN}{'─'*62}")
    print(f"  🎵  Julia Wolf · Streaming Analytics Report")
    print(f"{'─'*62}{RESET}")

    s = processor.summary()
    print(f"\n{BOLD}Pipeline Stats{RESET}")
    print(f"  Total events processed : {GREEN}{s['total_events_processed']:,}{RESET}")
    print(f"  Unique tracks streamed : {s['unique_tracks_streamed']}")
    print(f"  Throughput (last 60s)  : {YELLOW}{s['throughput_eps']} events/sec{RESET}")

    print(f"\n{BOLD}Track Leaderboard  (all time){RESET}")
    tracks = processor.top_tracks(len(CATALOG))
    max_streams = tracks[0].total_streams if tracks else 1

    for i, t in enumerate(tracks, 1):
        medal = ["🥇","🥈","🥉"][i-1] if i <= 3 else f" {i}."
        b = bar(t.total_streams, max_streams)
        pct_str = f"{t.completion_pct:5.1f}%"
        print(f"  {medal} {t.title:<22} {BLUE}{b}{RESET} {t.total_streams:>5} streams  "
              f"complete:{GREEN}{pct_str}{RESET}  top:{t.top_platform}")

    print(f"\n{BOLD}Top Track Deep-Dive  →  {tracks[0].title}{RESET}")
    top = tracks[0]
    print(f"  Unique listeners  : {top.unique_users:,}")
    print(f"  Avg listen time   : {top.avg_listen_s:.1f}s / "
          f"{processor._catalog_map[top.track_id]['duration_s']}s total")
    print(f"  Completion rate   : {top.completion_pct}%")
    print(f"  Top platform      : {top.top_platform}")
    print(f"  Top region        : {top.top_region}")

    print(f"\n{CYAN}{'─'*62}{RESET}\n")


# ---------------------------------------------------------------------------
# 6. MAIN  —  simulate three "waves" of streaming activity
# ---------------------------------------------------------------------------

def main():
    print(f"\n{BOLD}Starting Julia Wolf Streaming Analytics Pipeline...{RESET}")
    print("Simulating event ingestion in 3 waves (new release, weekend surge, steady state)\n")

    producer  = EventProducer(seed=7)
    processor = StreamProcessor()

    waves = [
        ("🚀 Wave 1 — New Release Spike",    1_200),
        ("📈 Wave 2 — Weekend Surge",         2_500),
        ("📊 Wave 3 — Steady State Baseline", 800),
    ]

    for label, n in waves:
        print(f"  {label}  ({n:,} events)...", end=" ", flush=True)
        batch = producer.batch(n)
        processor.process_batch(batch)
        print(f"done  |  running total: {processor._total_processed:,}")
        time.sleep(0.05)

    print_report(processor)

    # Export JSON snapshot
    snapshot = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "artist": "Julia Wolf",
        "summary": processor.summary(),
        "tracks": [
            {k: v for k, v in asdict(s).items() if not k.startswith("_")}
            for s in processor.top_tracks(len(CATALOG))
        ],
    }
    out_path = "/mnt/user-data/outputs/julia_wolf_analytics.json"
    with open(out_path, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"  📁 JSON snapshot exported → julia_wolf_analytics.json\n")


if __name__ == "__main__":
    main()
