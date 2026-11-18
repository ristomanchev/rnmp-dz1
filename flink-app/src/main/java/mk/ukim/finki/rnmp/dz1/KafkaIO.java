package mk.ukim.finki.rnmp.dz1;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/** Kafka source (JSON -> SensorReading) and Kafka sink (String -> topic). */
public final class KafkaIO {

    private KafkaIO() {}

    /**
     * @param offsets "earliest" = read everything that is already in the topic;
     *                "latest"   = read only the messages that arrive AFTER the application starts.
     */
    public static KafkaSource<SensorReading> source(String bootstrap, String topic, String offsets) {
        OffsetsInitializer start = switch (offsets) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            default -> throw new IllegalArgumentException("--offsets must be earliest or latest, not: " + offsets);
        };
        return KafkaSource.<SensorReading>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("air-quality-" + offsets)
                .setStartingOffsets(start)
                // only the value of the Kafka message is JSON; the key (sensorId) is not needed, it is in the JSON too
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SensorReading.class))
                .build();
    }

    /**
     * Sink for ready-made JSON strings (we build the JSON ourselves).
     *
     * The Kafka timestamp of the message is null, so Kafka uses the CURRENT time. The default serializer
     * (as in the class demo) uses the event time (September 2026): Kafka considers such messages older
     * than 7 days (retention) and deletes them within a few minutes.
     */
    public static KafkaSink<String> stringSink(String bootstrap, String topic) {
        KafkaRecordSerializationSchema<String> serializer = (json, context, eventTime) ->
                new ProducerRecord<>(topic, json.getBytes(StandardCharsets.UTF_8));
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(serializer)
                .build();
    }
}
