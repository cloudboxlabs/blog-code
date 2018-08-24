package com.cloudboxlabs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class TurnoverRatio {
    static final String HIGH_TURNOVER_TOPIC = "top-N-high-turnover";


    public static void main(String[] args) {
        final KafkaStreams streams = createStreams("localhost:9092", "/tmp/kafka-streams-1");
        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams createStreams(final String bootstrapServers,
                                             final String stateDir) {

        final Comparator<Integer> comparator =
                (o1, o2) -> o2 - o1;

        final Properties streamsConfiguration = new Properties();
        // unique app id on kafka cluster
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "citi-bike-turnover");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "citi-bike-turnover-client");
        // kafka broker address
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // local state store
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // consumer from the beginning of the topic or last offset
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // override default serdes
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Get the stream of station statuses
        ObjectMapper mapper = new ObjectMapper();
        KStream<String, String> statusStream = builder.stream(
                CitiBikeStationStatusAPI.topicName,
                Consumed.with(Serdes.String(), Serdes.String()))
                .map((station_id, v) -> {
                    try {
                        CitiBikeStationStatusAPI.StationStatus status = mapper.readValue(
                                v, CitiBikeStationStatusAPI.StationStatus.class);
                        return new KeyValue<>(station_id, Integer.toString(status.num_bikes_available));
                    } catch (Exception e) {
                        throw new RuntimeException("Deserialize error" + e);
                    }
                });

        // Build KTable of station information
        KTable<String, String> stationInfo = builder.table(CitiBikeStationInfoAPI.topicName);
        KTable<String, CitiBikeStationInfoAPI.StationInfo> stationInfoTable = stationInfo
                .mapValues(v -> {
                    try {
                        CitiBikeStationInfoAPI.StationInfo info = mapper.readValue(
                                v, CitiBikeStationInfoAPI.StationInfo.class);
                        return info;
                    } catch (Exception e) {
                        throw new RuntimeException("Deserialize error" + e);
                    }
                });

        // aggregate hourly over the net change in number of bikes
        final KTable<Windowed<String>, String> netDeltaTable = statusStream
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(60)))
                .aggregate(
                        // the initializer
                        () -> "0,0",

                        // the "add" aggregator
                        (stationId, record, accum) -> {
                            Integer newValue = Integer.parseInt(record);
                            Integer netDelta = Integer.parseInt(accum.split(",")[0]);
                            Integer lastValue = Integer.parseInt(accum.split(",")[1]);
                            Integer delta = Math.abs(newValue - lastValue);
                            return (netDelta + delta) + "," + newValue;
                        }
                );

        // join station information table to get station capacity and calculate turnover ratio
        final KStream<String, String> ratio = netDeltaTable
                .toStream()
                .map((windowedKey, v) -> new KeyValue<>(windowedKey.key(), v))
                .join(stationInfoTable, (delta, info) ->  delta + "," + info.capacity)
                .map((k, v) -> {
                    Integer delta = Integer.parseInt(v.split(",")[0]);
                    Integer capacity = Integer.parseInt(v.split(",")[2]);

                    if (capacity == 0) {
                       return new KeyValue<>(k, "NA");
                    }
                    Double turnover = delta / (double)capacity * 100.0;
                    return new KeyValue<>(k, String.format("%.2f", turnover));
                });

        ratio
                .map((k, v) -> new KeyValue<>(k, "station_id: " + k + ", turnover: " + v + "%"))
                .to(HIGH_TURNOVER_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
