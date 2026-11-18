"""
Producer for Sensor.Community (PMS5003) air-quality readings.

Reads the readings from the monthly zip and sends them to Kafka as JSON:
  * in time order, with the original timestamp (event time)
  * Kafka key = sensorId
  * accelerated: --speedup 60 means 1 hour of data in 1 minute
  * a fraction of the messages (--ooo-ratio) is sent later, by up to --max-delay-sec
    seconds of DATA TIME, so that watermarks make sense.

Example:
  python producer/producer.py --speedup 60 --hours 6
"""
import argparse
import csv
import heapq
import io
import json
import random
import time
import zipfile
from datetime import datetime, timezone


def parse_args():
    p = argparse.ArgumentParser(description="Sensor.Community -> Kafka producer")
    p.add_argument("--file", default="data/2026-09_pms5003.zip", help="monthly zip from archive.sensor.community")
    p.add_argument("--topic", default="air-quality")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--speedup", type=float, default=60.0,
                   help="how many times faster than real time (60 = 1 hour in 1 minute); 0 = as fast as possible")
    p.add_argument("--hours", type=float, default=6.0, help="how many hours of data to send")
    p.add_argument("--ooo-ratio", type=float, default=0.05, help="fraction of the messages sent with a delay (0-1)")
    p.add_argument("--max-delay-sec", type=float, default=180.0,
                   help="maximum delay, in seconds of DATA TIME")
    p.add_argument("--seed", type=int, default=42, help="same seed = same messages in the same order")
    p.add_argument("--dry-run", action="store_true", help="only print, do not send to Kafka")
    return p.parse_args()


def parse_pm(value):
    """'12.5' -> 12.5; 'unavailable', empty or a value outside the sensor range -> None"""
    try:
        v = float(value)
    except ValueError:
        return None
    return v if 0 <= v <= 1000 else None


def read_readings(path, hours):
    """Yields the readings one by one (the CSV is already sorted by time), only the first `hours` hours."""
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"), delimiter=";")
            first_ts = None
            for row in reader:
                pm10, pm25 = parse_pm(row["P1"]), parse_pm(row["P2"])
                if pm10 is None or pm25 is None:
                    continue
                try:
                    # the time in the file is UTC, without a time zone marker
                    ts = int(datetime.fromisoformat(row["timestamp"])
                             .replace(tzinfo=timezone.utc).timestamp() * 1000)
                    lat, lon = float(row["lat"]), float(row["lon"])
                except ValueError:
                    continue
                if first_ts is None:
                    first_ts = ts
                if ts - first_ts >= hours * 3600 * 1000:
                    break
                yield {
                    "sensorId": row["sensor_id"],
                    "timestamp": ts,
                    "pm10": pm10,
                    "pm25": pm25,
                    "lat": lat,
                    "lon": lon,
                }


def fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class Sender:
    """Sends one message in real time: waits until its (accelerated) time has come."""

    def __init__(self, args):
        self.args = args
        self.producer = None
        if not args.dry_run:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=args.bootstrap,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=20,
            )
        self.wall_start = None
        self.data_start = None
        self.sent = 0
        self.delayed = 0
        self.max_seen = 0
        self.max_lateness_ms = 0

    def send(self, send_at, reading):
        if self.wall_start is None:
            self.wall_start, self.data_start = time.time(), send_at
        if self.args.speedup > 0:
            target = self.wall_start + (send_at - self.data_start) / 1000 / self.args.speedup
            pause = target - time.time()
            if pause > 0:
                time.sleep(pause)

        if self.producer:
            self.producer.send(self.args.topic, key=reading["sensorId"], value=reading)
        else:
            print(json.dumps(reading))

        # statistics: how far behind the newest sent event this one is (this is what the watermark sees)
        ts = reading["timestamp"]
        if send_at > ts:
            self.delayed += 1
        self.max_lateness_ms = max(self.max_lateness_ms, self.max_seen - ts)
        self.max_seen = max(self.max_seen, ts)
        self.sent += 1
        if self.sent % 5000 == 0:
            print(f"  sent {self.sent}, data time {fmt(ts)}, "
                  f"{time.time() - self.wall_start:.0f} s since start", flush=True)

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()


def main():
    args = parse_args()
    random.seed(args.seed)
    max_delay_ms = int(args.max_delay_sec * 1000)
    sender = Sender(args)

    # heap = messages waiting to be sent, ordered by the time they should leave.
    # Normal message: send_at = timestamp. Delayed message: send_at = timestamp + delay,
    # so it leaves AFTER newer events -> out of order.
    heap = []
    try:
        for seq, r in enumerate(read_readings(args.file, args.hours)):
            delay = random.randint(1, max_delay_ms) if random.random() < args.ooo_ratio else 0
            heapq.heappush(heap, (r["timestamp"] + delay, seq, r))
            while heap and heap[0][0] <= r["timestamp"]:
                send_at, _, ready = heapq.heappop(heap)
                sender.send(send_at, ready)
        while heap:
            send_at, _, ready = heapq.heappop(heap)
            sender.send(send_at, ready)
    except KeyboardInterrupt:
        print("Interrupted.")
    finally:
        sender.close()

    wall = time.time() - sender.wall_start if sender.wall_start else 0
    print(f"Done: {sender.sent} messages to topic '{args.topic}', {wall:.1f} s.")
    print(f"Delayed: {sender.delayed} ({100 * sender.delayed / max(sender.sent, 1):.1f}%), "
          f"max out-of-orderness: {sender.max_lateness_ms / 1000:.1f} s (data time).")


if __name__ == "__main__":
    main()
