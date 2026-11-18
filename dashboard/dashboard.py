"""
Live dashboard for the outputs of the Flink application: aq-stats (windows) and aq-alerts (alerts).

  .venv/bin/streamlit run dashboard/dashboard.py

Opens in the browser (http://localhost:8501) and refreshes every 2 seconds.
The sidebar selects which run to show (the value of --run) and whether to read
the topics from the beginning or only the new results (live).
"""
import json
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Color by the average PM2.5 in the window (µg/m³); 25 is the alarm threshold
COLORS = [(15, "#2e7d32"), (25, "#f9a825"), (50, "#ef6c00"), (float("inf"), "#c62828")]
OFFLINE_COLOR = "#9e9e9e"


def pm_color(pm25):
    return next(color for limit, color in COLORS if pm25 <= limit)


def fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%d.%m %H:%M")


def active(alerts, on, off):
    """Sensors whose last event of the pair (on, off) is `on`, e.g. POLLUTION_ALARM without ALARM_CLEARED."""
    if alerts.empty:
        return set()
    pairs = alerts[alerts["type"].isin([on, off])].sort_values("eventTime")
    last = pairs.groupby("sensorId")["type"].last()
    return set(last[last == on].index)


st.set_page_config(page_title="Air quality", layout="wide")
st.title("Air quality: live results from Flink")

with st.sidebar:
    run = st.text_input("Run (--run)", value="", help="Empty = topics aq-stats and aq-alerts")
    from_start = st.checkbox("Read from the beginning of the topics", value=False,
                             help="Off = only the results that arrive from now on (live)")
    chosen_sensor = st.text_input("Sensor for the chart", value="", help="Empty = the most polluted sensor")
    bootstrap = st.text_input("Kafka", value="localhost:9092")

suffix = f"-{run}" if run else ""
stats_topic, alerts_topic = f"aq-stats{suffix}", f"aq-alerts{suffix}"
settings = (stats_topic, alerts_topic, from_start, bootstrap)

# A new consumer (and empty data) every time the settings change
if st.session_state.get("settings") != settings:
    if "consumer" in st.session_state:
        st.session_state.consumer.close()
        del st.session_state.consumer
    try:
        st.session_state.consumer = KafkaConsumer(
            stats_topic, alerts_topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest" if from_start else "latest",
            value_deserializer=json.loads,
        )
    except NoBrokersAvailable:
        st.error(f"Cannot connect to Kafka ({bootstrap}). Is docker compose running?")
        st.stop()
    st.session_state.settings = settings
    st.session_state.stats = []
    st.session_state.alerts = []


@st.fragment(run_every="2s")
def live():
    """Runs every 2 seconds: reads the new messages and redraws the results."""
    for tp, records in st.session_state.consumer.poll(timeout_ms=500, max_records=20000).items():
        target = st.session_state.stats if tp.topic == stats_topic else st.session_state.alerts
        target.extend(r.value for r in records)

    stats = pd.DataFrame(st.session_state.stats)
    alerts = pd.DataFrame(st.session_state.alerts)
    if stats.empty and alerts.empty:
        st.info(f"Waiting for results in {stats_topic} and {alerts_topic}. "
                "Start the Flink application, then the producer.")
        return

    polluted = active(alerts, "POLLUTION_ALARM", "ALARM_CLEARED")
    offline = active(alerts, "SENSOR_OFFLINE", "SENSOR_BACK_ONLINE")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Data up to (UTC)", fmt(stats["windowEnd"].max()) if not stats.empty else "-")
    c2.metric("Windows", len(stats))
    c3.metric("Sensors", stats["sensorId"].nunique() if not stats.empty else 0)
    c4.metric("Polluted now", len(polluted), help="Sensors with POLLUTION_ALARM and no ALARM_CLEARED yet")
    c5.metric("Sensors offline", len(offline), help="Sensors with SENSOR_OFFLINE and no SENSOR_BACK_ONLINE yet")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Latest window per sensor")
        if not stats.empty:
            latest = stats.sort_values("windowStart").groupby("sensorId").tail(1).copy()
            latest["color"] = [OFFLINE_COLOR if s in offline else pm_color(v)
                               for s, v in zip(latest["sensorId"], latest["avgPm25"])]
            st.map(latest, latitude="lat", longitude="lon", color="color", size=2000)
            st.caption("Average PM2.5: green ≤ 15, yellow ≤ 25, orange ≤ 50, red > 50 µg/m³; grey = offline")
    with right:
        st.subheader("Latest alerts")
        if alerts.empty:
            st.write("No alerts yet.")
        else:
            recent = alerts.sort_values("emittedAt", ascending=False).head(15)
            st.dataframe(pd.DataFrame({
                "type": recent["type"],
                "sensor": recent["sensorId"],
                "time": recent["eventTime"].map(fmt),
                "since": recent["since"].map(fmt),
                "PM2.5": recent["pm25"],
            }), hide_index=True)
            st.caption(", ".join(f"{t}: {n}" for t, n in alerts["type"].value_counts().items()))

    if not stats.empty:
        ranking = stats.groupby("sensorId")["avgPm25"].max().sort_values(ascending=False)
        sensor = chosen_sensor if chosen_sensor in ranking.index else ranking.index[0]
        st.subheader(f"PM2.5 over time, sensor {sensor}")
        one = stats[stats["sensorId"] == sensor].sort_values("windowStart")
        one = one.assign(time=pd.to_datetime(one["windowStart"], unit="ms", utc=True))
        st.line_chart(one, x="time", y=["avgPm25", "maxPm25"], y_label="µg/m³")


live()
