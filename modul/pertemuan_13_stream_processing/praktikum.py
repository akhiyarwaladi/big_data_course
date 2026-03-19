# ==============================================================================
# PERTEMUAN 13 — Stream Processing & Real-time Analytics
# Studi Kasus: Social Media Stream + IoT Sensor Monitoring
# Platform: Google Colab (simulasi Kafka concepts)
# ==============================================================================

# %% [1] Setup
!pip install duckdb polars -q

import duckdb
import polars as pl
import random, json, time, threading, queue
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

random.seed(42)
print("✅ Ready!")

# %% [2] Simple Message Broker (simulasi Apache Kafka)
class MessageBroker:
    """Simulasi Kafka: topics, producers, consumers, offsets."""
    def __init__(self):
        self.topics = defaultdict(list)
        self.offsets = defaultdict(int)
        self._lock = threading.Lock()

    def produce(self, topic, event):
        with self._lock:
            event['_offset'] = len(self.topics[topic])
            event['_ts'] = datetime.now().isoformat()
            self.topics[topic].append(event)

    def consume(self, topic, group='default', batch=100):
        with self._lock:
            key = f"{topic}:{group}"
            start = self.offsets[key]
            events = self.topics[topic][start:start+batch]
            self.offsets[key] = start + len(events)
            return events

    def size(self, topic):
        return len(self.topics[topic])

broker = MessageBroker()

# %% [3] STUDI KASUS 1: Social Media Event Stream
USERS = [f'user_{i:04d}' for i in range(1, 501)]
PLATFORMS = ['Instagram', 'TikTok', 'Twitter/X', 'YouTube']
EVENTS = ['post', 'like', 'comment', 'share', 'view']
HASHTAGS = ['#BigData','#AI','#Python','#Viral','#Tech','#Kuliah',
            '#OOTD','#FoodPorn','#Gaming','#Trending','#Memes',
            '#Startup','#Indonesia','#DataScience','#WFH']
SENTIMENTS = ['positive', 'negative', 'neutral']

def gen_social_event():
    etype = random.choices(EVENTS, weights=[.05,.40,.15,.10,.30])[0]
    e = {
        'event_type': etype,
        'user_id': random.choice(USERS),
        'platform': random.choices(PLATFORMS, weights=[.35,.30,.20,.15])[0],
        'hashtags': random.sample(HASHTAGS, k=random.randint(0,3)),
    }
    if etype in ('post','comment'):
        e['sentiment'] = random.choices(SENTIMENTS, weights=[.50,.15,.35])[0]
    return e

# Produce 50K events
N_EVENTS = 50_000
t0 = time.time()
for _ in range(N_EVENTS):
    broker.produce('social', gen_social_event())
print(f"✅ Produced {N_EVENTS:,} social events in {time.time()-t0:.2f}s")
print(f"Sample: {json.dumps(broker.topics['social'][0], indent=2)}")

# %% [4] Tumbling Window Processor
class TumblingWindow:
    """Fixed-size, non-overlapping windows."""
    def __init__(self, size):
        self.size = size
        self.buf = []
        self.results = []

    def add(self, event):
        self.buf.append(event)
        if len(self.buf) >= self.size:
            self.results.append(self._compute())
            self.buf = []

    def _compute(self):
        events = self.buf
        types = defaultdict(int)
        platforms = defaultdict(int)
        hashtags = defaultdict(int)
        for e in events:
            types[e['event_type']] += 1
            platforms[e['platform']] += 1
            for h in e.get('hashtags', []):
                hashtags[h] += 1

        sents = [e['sentiment'] for e in events if 'sentiment' in e]
        neg_pct = sum(1 for s in sents if s=='negative') / max(len(sents),1) * 100

        return {
            'window': len(self.results),
            'events': len(events),
            'users': len(set(e['user_id'] for e in events)),
            'top_platform': max(platforms, key=platforms.get),
            'top_event': max(types, key=types.get),
            'top_hashtag': max(hashtags, key=hashtags.get) if hashtags else '-',
            'neg_pct': round(neg_pct, 1),
            'likes': types.get('like', 0),
        }

# Process
tw = TumblingWindow(2500)
all_events = broker.consume('social', batch=N_EVENTS)
for e in all_events:
    tw.add(e)

print(f"\n📊 TUMBLING WINDOW RESULTS ({len(tw.results)} windows)")
print(f"{'Win':>4} {'Users':>6} {'Platform':>12} {'#Hashtag':>15} {'Neg%':>6} {'Likes':>6}")
print("-" * 60)
for r in tw.results:
    print(f"{r['window']:4d} {r['users']:6d} {r['top_platform']:>12s} "
          f"{r['top_hashtag']:>15s} {r['neg_pct']:5.1f}% {r['likes']:6d}")

# %% [5] Sliding Window Processor
class SlidingWindow:
    """Overlapping windows: window_size with step slide_size."""
    def __init__(self, window_size, slide_size):
        self.ws = window_size
        self.ss = slide_size

    def compute(self, events):
        results = []
        for start in range(0, len(events) - self.ws + 1, self.ss):
            window = events[start:start + self.ws]
            ht = defaultdict(int)
            for e in window:
                for h in e.get('hashtags', []):
                    ht[h] += 1
            top3 = sorted(ht.items(), key=lambda x: -x[1])[:3]
            engagement = sum(1 for e in window if e['event_type'] in ('like','comment','share'))
            results.append({
                'start': start, 'end': start + self.ws,
                'users': len(set(e['user_id'] for e in window)),
                'engagement_pct': round(engagement / len(window) * 100, 1),
                'top3': [h[0] for h in top3],
            })
        return results

sw = SlidingWindow(5000, 2500)
sr = sw.compute(all_events)

print(f"\n📊 SLIDING WINDOW (5000 events, slide 2500)")
for r in sr:
    print(f"[{r['start']:5d}-{r['end']:5d}] Users:{r['users']:4d} "
          f"Engagement:{r['engagement_pct']:5.1f}%  🔥 {', '.join(r['top3'])}")

# %% [6] STUDI KASUS 2: IoT Sensor Monitoring
WAREHOUSES = {
    'WH-JKT': {'name': 'Gudang Jakarta', 'type': 'cold', 'target': 4},
    'WH-SBY': {'name': 'Gudang Surabaya', 'type': 'dry', 'target': 25},
    'WH-BDG': {'name': 'Gudang Bandung', 'type': 'cold', 'target': 4},
}

def gen_sensor_event():
    wh = random.choice(list(WAREHOUSES.keys()))
    info = WAREHOUSES[wh]
    is_anomaly = random.random() < 0.05
    if is_anomaly:
        temp = round(info['target'] + random.uniform(5, 15), 1)
    else:
        temp = round(info['target'] + random.gauss(0, 1), 1)
    return {
        'warehouse': wh,
        'wh_name': info['name'],
        'wh_type': info['type'],
        'temp': temp,
        'humidity': round(random.uniform(40, 95 if is_anomaly else 65), 1),
        'is_anomaly': is_anomaly,
    }

N_SENSOR = 20_000
for _ in range(N_SENSOR):
    broker.produce('sensors', gen_sensor_event())
print(f"\n✅ Produced {N_SENSOR:,} sensor events")

# %% [7] Anomaly Detection Consumer
class AnomalyDetector:
    def __init__(self, threshold=3):
        self.threshold = threshold
        self.anomaly_buf = defaultdict(list)
        self.cumulative_count = defaultdict(int)  # track total anomalies per warehouse
        self.alerts = []

    def process(self, event):
        if event.get('is_anomaly'):
            wh = event['warehouse']
            self.anomaly_buf[wh].append(event)
            self.cumulative_count[wh] += 1
            if len(self.anomaly_buf[wh]) >= self.threshold:
                alert = {
                    'warehouse': wh,
                    'name': event['wh_name'],
                    'temp': event['temp'],
                    'count': self.cumulative_count[wh],
                    'severity': 'HIGH' if self.cumulative_count[wh] >= 15 else 'MEDIUM',
                }
                self.alerts.append(alert)
                self.anomaly_buf[wh] = []
                return alert
        return None

detector = AnomalyDetector(threshold=3)
sensor_events = broker.consume('sensors', batch=N_SENSOR)

for e in sensor_events:
    alert = detector.process(e)
    if alert:
        sev = '🔴' if alert['severity'] == 'HIGH' else '🟡'
        print(f"{sev} ALERT [{alert['severity']}] {alert['name']}: "
              f"temp={alert['temp']}°C ({alert['count']} anomalies)")

print(f"\n📊 Total alerts: {len(detector.alerts)}")

# %% [8] Analyze sensor data with DuckDB
con = duckdb.connect()
df_s = pl.DataFrame([
    {'warehouse': e['warehouse'], 'wh_type': e['wh_type'],
     'temp': e['temp'], 'humidity': e['humidity'],
     'is_anomaly': e['is_anomaly']}
    for e in sensor_events
])
con.register('sensors', df_s)

print("\n🌡️ Sensor Analytics:")
con.sql("""
    SELECT warehouse, wh_type,
           ROUND(AVG(temp), 1) AS avg_temp,
           ROUND(MIN(temp), 1) AS min_temp,
           ROUND(MAX(temp), 1) AS max_temp,
           COUNT(*) AS readings,
           SUM(is_anomaly::INT) AS anomalies,
           ROUND(SUM(is_anomaly::INT)*100.0/COUNT(*), 1) AS anomaly_pct
    FROM sensors GROUP BY warehouse, wh_type ORDER BY anomaly_pct DESC
""").show()
con.close()

# %% [9] Event Sourcing Pattern Demo
print("\n📚 EVENT SOURCING: Shopping Cart")
print("=" * 50)

class CartEventStore:
    def __init__(self):
        self.events = []
    def add(self, uid, action, pid, qty=1, price=0):
        self.events.append({'user': uid, 'action': action,
                            'product': pid, 'qty': qty, 'price': price,
                            'ts': datetime.now().isoformat()})
    def state(self, uid):
        cart = {}
        for e in self.events:
            if e['user'] != uid: continue
            pid = e['product']
            if e['action'] == 'add':
                cart[pid] = cart.get(pid, {'qty':0, 'price':e['price']})
                cart[pid]['qty'] += e['qty']
            elif e['action'] == 'remove':
                cart.pop(pid, None)
            elif e['action'] == 'checkout':
                cart = {}
        return cart
    def total(self, uid):
        return sum(i['price']*i['qty'] for i in self.state(uid).values())

store = CartEventStore()
store.add('andi', 'add', 'iPhone-15', 1, 15_000_000)
store.add('andi', 'add', 'AirPods', 1, 2_500_000)
store.add('andi', 'add', 'Case', 2, 150_000)
store.add('andi', 'remove', 'AirPods')
store.add('andi', 'add', 'Case', 1, 150_000)

print("Cart Andi:")
for pid, info in store.state('andi').items():
    print(f"  {pid}: {info['qty']}x @ Rp {info['price']:,} = Rp {info['price']*info['qty']:,}")
print(f"  Total: Rp {store.total('andi'):,}")
print(f"  Events stored: {len(store.events)} (full audit trail!)")

# %% [10] Visualization Dashboard
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Real-time Analytics Dashboard', fontsize=14, fontweight='bold')

# 1. Likes per window
wins = [r['window'] for r in tw.results]
likes = [r['likes'] for r in tw.results]
axes[0][0].plot(wins, likes, 'b-o', ms=4)
axes[0][0].set_title('Likes per Window'); axes[0][0].set_xlabel('Window')
axes[0][0].grid(alpha=.3)

# 2. Unique users
users = [r['users'] for r in tw.results]
axes[0][1].bar(wins, users, color='green', alpha=.7)
axes[0][1].axhline(np.mean(users), color='red', ls='--', label='avg')
axes[0][1].set_title('Unique Users per Window'); axes[0][1].legend()

# 3. Negative sentiment
neg = [r['neg_pct'] for r in tw.results]
colors = ['red' if n>20 else 'orange' if n>15 else 'green' for n in neg]
axes[1][0].bar(wins, neg, color=colors, alpha=.8)
axes[1][0].axhline(15, color='orange', ls='--'); axes[1][0].axhline(20, color='red', ls='--')
axes[1][0].set_title('Negative Sentiment % (Monitor!)')

# 4. Platform pie (last window)
last = all_events[-2500:]
pd_dist = defaultdict(int)
for e in last: pd_dist[e['platform']] += 1
axes[1][1].pie(pd_dist.values(), labels=pd_dist.keys(), autopct='%1.0f%%')
axes[1][1].set_title('Platform Distribution (Latest)')

plt.tight_layout(); plt.show()
print("✅ Praktikum selesai!")
