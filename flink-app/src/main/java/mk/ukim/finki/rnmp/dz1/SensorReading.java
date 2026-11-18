package mk.ukim.finki.rnmp.dz1;

import java.time.Instant;
import java.util.Locale;

/**
 * One sensor reading, exactly like the JSON message sent by the producer.
 * Flink POJO: public class, public no-argument constructor, public fields.
 * The field names must be the SAME as the keys in the JSON,
 * otherwise JsonDeserializationSchema throws an error.
 */
public class SensorReading {
    public String sensorId;
    public long timestamp;   // event time: when the reading was taken, epoch ms (UTC)
    public double pm10;      // µg/m³
    public double pm25;      // µg/m³
    public double lat;
    public double lon;

    public SensorReading() {}

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "sensor %-6s %s  PM2.5=%6.1f  PM10=%6.1f",
                sensorId, Instant.ofEpochMilli(timestamp), pm25, pm10);
    }
}
