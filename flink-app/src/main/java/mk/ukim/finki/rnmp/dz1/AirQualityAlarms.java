package mk.ukim.finki.rnmp.dz1;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Locale;

/**
 * Event-driven module: for EACH sensor separately (keyBy sensorId) it tracks two rules.
 *
 * 1) Sustained pollution: PM2.5 above the threshold, without interruption, for at least sustainMs.
 *    - first high reading            -> remember it (highStart), timer at highStart + sustainMs
 *    - the timer fires               -> POLLUTION_ALARM
 *    - low reading after the alarm   -> ALARM_CLEARED
 *    - low reading before the timer  -> the streak is broken, no alarm
 *
 * 2) Sensor went silent: no new reading for longer than offlineMs.
 *    - every reading sets a timer at timestamp + offlineMs
 *    - the timer fires and no newer reading has arrived -> SENSOR_OFFLINE
 *    - the next reading after that                      -> SENSOR_BACK_ONLINE
 *
 * The timers are in EVENT time: they fire when the WATERMARK passes the deadline.
 *
 * Two things to watch out for:
 *  - We never delete timers. Flink keeps only ONE timer per key and timestamp, so deleting the timer
 *    of one rule could also delete the timer of the other rule. Instead, onTimer checks whether the
 *    timer is still valid according to the state; stale timers are ignored.
 *  - When the watermark lags behind (e.g. earliest: all the data is read within a second), a newer
 *    reading can arrive BEFORE the timer of an older one fires. That is why the same rule is also
 *    checked when the reading arrives, with the same event time as the timer. This way the results
 *    are the same no matter how fast the data is read.
 */
public class AirQualityAlarms extends KeyedProcessFunction<String, SensorReading, String> {

    private final double threshold;
    private final long sustainMs;
    private final long offlineMs;

    // Per-sensor state (Flink keeps it separately for every key)
    private transient ValueState<SensorReading> last;        // the newest reading
    private transient ValueState<SensorReading> highStart;   // first reading of the current streak above the threshold
    private transient ValueState<Boolean> alarmOn;           // whether POLLUTION_ALARM was sent for the streak
    private transient ValueState<Boolean> offline;           // whether SENSOR_OFFLINE was sent

    public AirQualityAlarms(double threshold, long sustainMs, long offlineMs) {
        this.threshold = threshold;
        this.sustainMs = sustainMs;
        this.offlineMs = offlineMs;
    }

    @Override
    public void open(OpenContext ctx) {   // Flink 2.x: OpenContext, not Configuration
        last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", SensorReading.class));
        highStart = getRuntimeContext().getState(new ValueStateDescriptor<>("high-start", SensorReading.class));
        alarmOn = getRuntimeContext().getState(new ValueStateDescriptor<>("alarm-on", Boolean.class));
        offline = getRuntimeContext().getState(new ValueStateDescriptor<>("offline", Boolean.class));
    }

    @Override
    public void processElement(SensorReading r, Context ctx, Collector<String> out) throws Exception {
        // A reading behind the watermark: the decisions for that time were already made (same as in the windows)
        if (r.timestamp <= ctx.timerService().currentWatermark()) return;

        // --- rule 2: sensor went silent ---
        SensorReading prev = last.value();
        if (prev == null || r.timestamp > prev.timestamp) {          // only newer readings move the deadline
            if (prev != null && r.timestamp - prev.timestamp > offlineMs) {
                if (!Boolean.TRUE.equals(offline.value())) {
                    // the timer has not fired yet (the watermark lags behind) -> alarm now, with the timer's time
                    out.collect(alert("SENSOR_OFFLINE", prev.timestamp + offlineMs, prev, prev.timestamp));
                }
                out.collect(alert("SENSOR_BACK_ONLINE", r.timestamp, r, prev.timestamp));
            }
            offline.clear();
            last.update(r);
            ctx.timerService().registerEventTimeTimer(r.timestamp + offlineMs);
        }

        // --- rule 1: sustained pollution ---
        SensorReading start = highStart.value();
        if (r.pm25 > threshold) {
            if (start == null) {                                      // a new streak starts
                highStart.update(r);
                ctx.timerService().registerEventTimeTimer(r.timestamp + sustainMs);
            }
        } else if (start != null && r.timestamp > start.timestamp) {  // a low reading breaks the streak
            boolean alarmed = Boolean.TRUE.equals(alarmOn.value());
            if (!alarmed && r.timestamp >= start.timestamp + sustainMs) {
                // the streak lasted long enough, but the timer has not fired yet -> alarm now, with the timer's time
                out.collect(alert("POLLUTION_ALARM", start.timestamp + sustainMs, start, start.timestamp));
                alarmed = true;
            }
            if (alarmed) {
                out.collect(alert("ALARM_CLEARED", r.timestamp, r, start.timestamp));
            }
            highStart.clear();
            alarmOn.clear();
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        // rule 2: is this the deadline of the NEWEST reading? (if not, a newer reading arrived -> stale timer)
        SensorReading l = last.value();
        if (l != null && ts == l.timestamp + offlineMs && !Boolean.TRUE.equals(offline.value())) {
            offline.update(true);
            out.collect(alert("SENSOR_OFFLINE", ts, l, l.timestamp));
        }

        // rule 1: is the streak that set this timer still going on?
        SensorReading start = highStart.value();
        if (start != null && ts == start.timestamp + sustainMs && !Boolean.TRUE.equals(alarmOn.value())) {
            alarmOn.update(true);
            out.collect(alert("POLLUTION_ALARM", ts, start, start.timestamp));
        }
    }

    /**
     * JSON for an alert.
     * eventTime = when the event happened (data time); since = since when the condition lasts;
     * pm25 = value of the reading related to the event (start of the streak, last reading before the silence...).
     */
    private String alert(String type, long eventTime, SensorReading r, long since) {
        return String.format(Locale.ROOT,
                "{\"type\":\"%s\",\"sensorId\":\"%s\",\"eventTime\":%d,\"eventTimeIso\":\"%s\",\"since\":%d,"
                        + "\"pm25\":%.2f,\"threshold\":%.1f,\"lat\":%.5f,\"lon\":%.5f,\"emittedAt\":%d}",
                type, r.sensorId, eventTime, Instant.ofEpochMilli(eventTime), since,
                r.pm25, threshold, r.lat, r.lon, System.currentTimeMillis());
    }
}
