"""
Quick data exploration: how frequent the events are and which PM2.5 threshold to choose.

  python producer/explore.py --hours 24
"""
import argparse
from collections import defaultdict

from producer import read_readings

p = argparse.ArgumentParser()
p.add_argument("--file", default="data/2026-09_pms5003.zip")
p.add_argument("--hours", type=float, default=24.0)
args = p.parse_args()

readings = list(read_readings(args.file, args.hours))
pm25 = sorted(r["pm25"] for r in readings)
n = len(pm25)
span_min = (readings[-1]["timestamp"] - readings[0]["timestamp"]) / 60000

per_sensor = defaultdict(list)
for r in readings:
    per_sensor[r["sensorId"]].append(r["timestamp"])
gaps = []
for times in per_sensor.values():
    gaps += [(b - a) / 1000 for a, b in zip(times, times[1:])]
gaps.sort()

print(f"Readings: {n} over {span_min / 60:.1f} hours = {n / span_min:.0f} per minute")
print(f"Sensors: {len(per_sensor)}")
print(f"Time between two readings of the same sensor: median {gaps[len(gaps) // 2]:.0f} s, "
      f"95th percentile {gaps[int(len(gaps) * 0.95)]:.0f} s")
print("PM2.5 percentiles:", {q: pm25[int(n * q / 100)] for q in (50, 75, 90, 95, 99)})
for t in (15, 25, 35, 50):
    print(f"  above {t:>2} µg/m³: {100 * sum(v > t for v in pm25) / n:5.1f}% of the readings")
