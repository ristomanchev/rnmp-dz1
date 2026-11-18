"""
Compares the results of several runs of the Flink application.

  .venv/bin/python results/compare.py latest earliest latest-60 latest-0 earliest-0

For every run (the value of --run) it reads the topics aq-stats-<name>, aq-alerts-<name>
and aq-late-<name> and prints: number of windows, alerts and late readings, duration and the
last window/alert. The first two runs are compared record by record, without emittedAt
(the real emission time is always different).
"""
import argparse
import json
from collections import Counter
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition


def read_topic(bootstrap, topic):
    """Reads all messages of the topic, from beginning to end. If the topic does not exist: empty list."""
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    if topic not in consumer.topics():
        consumer.close()
        return []
    tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)
    end = consumer.end_offsets(tps)
    rows, empty_polls = [], 0
    while empty_polls < 10 and any(consumer.position(tp) < end[tp] for tp in tps):
        batch = consumer.poll(timeout_ms=1000)
        empty_polls = 0 if batch else empty_polls + 1
        for records in batch.values():
            rows += [json.loads(r.value) for r in records]
    consumer.close()
    return rows


def fmt(ms, pattern="%d.%m %H:%M"):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(pattern)


def without_emitted(rows):
    return Counter(json.dumps({k: v for k, v in r.items() if k != "emittedAt"}, sort_keys=True) for r in rows)


p = argparse.ArgumentParser()
p.add_argument("runs", nargs="+", help="names of the runs (the value of --run), e.g. latest earliest")
p.add_argument("--bootstrap", default="localhost:9092")
args = p.parse_args()

data = {}
print(f"{'run':12} {'windows':>8} {'alerts':>7} {'late':>6} {'duration':>9}   {'last window':21} last alert")
for run in args.runs:
    stats = read_topic(args.bootstrap, f"aq-stats-{run}")
    alerts = read_topic(args.bootstrap, f"aq-alerts-{run}")
    late = read_topic(args.bootstrap, f"aq-late-{run}")
    data[run] = {"windows": stats, "alerts": alerts, "late": late}

    emitted = [r["emittedAt"] for r in stats + alerts]
    duration = (max(emitted) - min(emitted)) / 1000 if emitted else 0   # first -> last result
    if stats:
        w = max(stats, key=lambda r: r["windowStart"])
        last_window = f"{fmt(w['windowStart'])}-{fmt(w['windowEnd'], '%H:%M')}"
    else:
        last_window = "-"
    last_alert = fmt(max(r["eventTime"] for r in alerts)) if alerts else "-"
    print(f"{run:12} {len(stats):8} {len(alerts):7} {len(late):6} {duration:8.1f}s   {last_window:21} {last_alert}")

if len(args.runs) >= 2:
    a, b = args.runs[0], args.runs[1]
    print(f"\n{a} vs {b}:")
    for name in ("windows", "alerts", "late"):
        ca, cb = without_emitted(data[a][name]), without_emitted(data[b][name])
        print(f"  {name:8} same: {'YES' if ca == cb else 'NO'}")
        for label, diff in ((f"only in {a}", ca - cb), (f"only in {b}", cb - ca)):
            for row in list(diff.elements())[:3]:
                print(f"      {label}: {row}")
