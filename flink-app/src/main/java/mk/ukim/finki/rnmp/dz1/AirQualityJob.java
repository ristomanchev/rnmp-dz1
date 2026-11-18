package mk.ukim.finki.rnmp.dz1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Locale;

/**
 * Air quality (Sensor.Community) - Flink application with two modules.
 *
 *   Kafka "air-quality" --> event time + watermarks --+--> [analytics] 15 min windows per sensor --> "aq-stats"
 *                                                     |                    late readings         --> "aq-late"
 *                                                     +--> [alarms] state + timers per sensor    --> "aq-alerts"
 *
 * Arguments (all optional):
 *   --offsets earliest|latest   where the Kafka source starts reading (default latest)
 *   --ooo-sec 180               watermark bound, in seconds of DATA TIME
 *   --run latest                name of the run: the output topics become aq-stats-latest etc.
 *   --pm25-threshold 25         PM2.5 threshold (µg/m³)
 *   --sustain-min 30            minutes above the threshold for POLLUTION_ALARM
 *   --offline-min 15            minutes without a reading for SENSOR_OFFLINE
 *   --window-min 15             window size of the analytics module
 *   --parallelism 4             parallelism (= number of partitions of the input topic)
 *   --print true|false          also print the results to the console
 *   --bootstrap localhost:9092  --input-topic air-quality
 */
public class AirQualityJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = Args.str(args, "--bootstrap", "localhost:9092");
        String inputTopic = Args.str(args, "--input-topic", "air-quality");
        String offsets = Args.str(args, "--offsets", "latest");
        String run = Args.str(args, "--run", "");
        String suffix = run.isEmpty() ? "" : "-" + run;
        long oooSec = Args.num(args, "--ooo-sec", 180);
        double threshold = Args.dbl(args, "--pm25-threshold", 25.0);
        long sustainMin = Args.num(args, "--sustain-min", 30);
        long offlineMin = Args.num(args, "--offline-min", 15);
        long windowMin = Args.num(args, "--window-min", 15);
        int parallelism = (int) Args.num(args, "--parallelism", 4);
        boolean print = Boolean.parseBoolean(Args.str(args, "--print", "true"));

        System.out.printf(Locale.ROOT, "Start: offsets=%s, bound=%d s, window=%d min, threshold=%.1f, output: aq-stats%s / aq-alerts%s%n",
                offsets, oooSec, windowMin, threshold, suffix, suffix);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // EVENT TIME: the time is the timestamp field of the reading, not when the message arrived.
        // Watermark = largest timestamp seen - bound. The producer delays by at most 180 s -> bound 180 s.
        // withIdleness is in REAL time: a partition without messages for 10 s does not hold back the watermark.
        WatermarkStrategy<SensorReading> watermarks = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(oooSec))
                .withTimestampAssigner((reading, kafkaTs) -> reading.timestamp)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<SensorReading> readings = env.fromSource(
                KafkaIO.source(bootstrap, inputTopic, offsets), watermarks, "kafka: " + inputTopic);

        // ---------- Module 1: windowed analytics ----------
        // Tumbling window: every reading belongs to EXACTLY one window, so a late reading
        // (its window was already emitted) goes to a side output instead of being silently lost.
        OutputTag<SensorReading> lateTag = new OutputTag<>("late-readings") {};

        SingleOutputStreamOperator<String> stats = readings
                .keyBy(r -> r.sensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(windowMin)))
                .sideOutputLateData(lateTag)
                .aggregate(new PmAggregate(), new StatsToJson())
                .name("window stats");

        stats.sinkTo(KafkaIO.stringSink(bootstrap, "aq-stats" + suffix)).name("kafka: aq-stats" + suffix);

        DataStream<String> late = stats.getSideOutput(lateTag)
                .map(r -> String.format(Locale.ROOT,
                        "{\"sensorId\":\"%s\",\"timestamp\":%d,\"pm25\":%.2f}", r.sensorId, r.timestamp, r.pm25))
                .name("late readings");
        late.sinkTo(KafkaIO.stringSink(bootstrap, "aq-late" + suffix)).name("kafka: aq-late" + suffix);

        // ---------- Module 2: event-driven alarms ----------
        DataStream<String> alerts = readings
                .keyBy(r -> r.sensorId)
                .process(new AirQualityAlarms(threshold, sustainMin * 60_000, offlineMin * 60_000))
                .name("alarms");

        alerts.sinkTo(KafkaIO.stringSink(bootstrap, "aq-alerts" + suffix)).name("kafka: aq-alerts" + suffix);

        if (print) {
            stats.print("STATS");
            alerts.print("ALERT");
            late.print("LATE");
        }

        env.execute("DZ1 - air quality (" + offsets + ")");
    }
}
