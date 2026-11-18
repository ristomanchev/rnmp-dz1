package mk.ukim.finki.rnmp.dz1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Locale;

/**
 * Called ONCE when the window closes: receives the finished accumulator from PmAggregate
 * and adds the window information (start, end). The result is a JSON string.
 */
public class StatsToJson extends ProcessWindowFunction<PmAggregate.Acc, String, String, TimeWindow> {

    @Override
    public void process(String sensorId, Context ctx, Iterable<PmAggregate.Acc> results, Collector<String> out) {
        PmAggregate.Acc a = results.iterator().next();   // with aggregate() there is exactly one element here
        TimeWindow w = ctx.window();
        out.collect(String.format(Locale.ROOT,
                "{\"sensorId\":\"%s\",\"windowStart\":%d,\"windowEnd\":%d,\"windowStartIso\":\"%s\","
                        + "\"count\":%d,\"avgPm25\":%.2f,\"maxPm25\":%.2f,\"avgPm10\":%.2f,\"maxPm10\":%.2f,"
                        + "\"lat\":%.5f,\"lon\":%.5f,\"emittedAt\":%d}",
                sensorId, w.getStart(), w.getEnd(), Instant.ofEpochMilli(w.getStart()),
                a.count, a.sumPm25 / a.count, a.maxPm25, a.sumPm10 / a.count, a.maxPm10,
                a.lat, a.lon, System.currentTimeMillis()));
    }
}
