package mk.ukim.finki.rnmp.dz1;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation in the window: every reading updates one small accumulator
 * (count, sums, maxima) instead of keeping all readings until the end of the window.
 */
public class PmAggregate implements AggregateFunction<SensorReading, PmAggregate.Acc, PmAggregate.Acc> {

    /** The accumulator. It is a POJO too, so Flink can store it efficiently in state. */
    public static class Acc {
        public long count;
        public double sumPm25;
        public double maxPm25;
        public double sumPm10;
        public double maxPm10;
        public double lat;
        public double lon;

        public Acc() {}
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(SensorReading r, Acc acc) {
        acc.count++;
        acc.sumPm25 += r.pm25;
        acc.maxPm25 = Math.max(acc.maxPm25, r.pm25);
        acc.sumPm10 += r.pm10;
        acc.maxPm10 = Math.max(acc.maxPm10, r.pm10);
        acc.lat = r.lat;
        acc.lon = r.lon;
        return acc;
    }

    @Override
    public Acc getResult(Acc acc) {
        return acc;
    }

    /** Only used by merging windows (session windows); implemented for completeness. */
    @Override
    public Acc merge(Acc a, Acc b) {
        a.count += b.count;
        a.sumPm25 += b.sumPm25;
        a.maxPm25 = Math.max(a.maxPm25, b.maxPm25);
        a.sumPm10 += b.sumPm10;
        a.maxPm10 = Math.max(a.maxPm10, b.maxPm10);
        return a;
    }
}
