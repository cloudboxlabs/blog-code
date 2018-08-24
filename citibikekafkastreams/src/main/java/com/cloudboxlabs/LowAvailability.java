package com.cloudboxlabs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;


public class LowAvailability {
    static final String LOW_BIKE_TOPIC = "low-bike-availability";


    public static void main(String[] args) {
        final KafkaStreams streams = createStreams("localhost:9092", "/tmp/kafka-streams");
        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams createStreams(final String bootstrapServers,
                                             final String stateDir) {

        final Properties streamsConfiguration = new Properties();
        // unique app id on kafka cluster
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "citi-bike-low-availability");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "citi-bike-low-availability-client");
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

        // stations with bike availability ratio (num_bikes_avail / capacity) < threshold
        KStream<String, String> outputStream = statusStream
                .leftJoin(stationInfoTable, (num_bikes, info) -> {
                    return new BikeStats(Integer.parseInt(num_bikes), info.capacity,
                            info.latitude, info.longitude);
                })
                .filter((k, stats) -> stats.availabilityRatio < 0.1)
                .map((k, stats) -> new KeyValue<>(k, "station_id: " + k +
                        ", longitude " + stats.longitude +
                        ", latitude " + stats.latitude +
                        ", bikes: " + stats.numBikesAvailable +
                        ", capacity: " + stats.stationCapacity +
                        ", ratio: " + String.format("%.2f", stats.availabilityRatio * 100) + "%"));

        // output to kafka topic
        outputStream
                .to(LOW_BIKE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private static class BikeStats {
        public final Integer numBikesAvailable;
        public final Integer stationCapacity;
        public final Float availabilityRatio;
        public final Float latitude;
        public final Float longitude;

        public BikeStats(Integer bikesAvailable, Integer capacity, Float lat, Float lng) {
            numBikesAvailable = bikesAvailable;
            stationCapacity = capacity;
            availabilityRatio = capacity == null ? 1 : bikesAvailable / (float) stationCapacity;
            latitude = lat;
            longitude = lng;
        }
    }

}
