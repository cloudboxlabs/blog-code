package com.cloudboxlabs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class CitiBikeStationStatusAPI {
    private static final String stationInformationUrl =
            "https://gbfs.citibikenyc.com/gbfs/en/station_status.json";
    public static final String topicName = "station_status";

    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /** URL for Citi bike API. */
    public static class CitiBikeURL extends GenericUrl {

        public CitiBikeURL(String encodedUrl) {
            super(encodedUrl);
        }

        @Key
        public String fields;
    }

    /** Represents a StationStatus feed. */
    public static class StationStatusFeed {
        @Key("ttl")
        public int ttl;

        @Key("data")
        public StationInfoData data;
    }

    /** Represents a data object in response */
    public static class StationInfoData {
        @Key
        public List<StationStatus> stations;
    }

    /** Represents a station status object */
    public static class StationStatus {
        @Key("station_id")
        public String stationId;

        @Key
        public int num_bikes_available;

        @Key
        public int num_bikes_disabled;

        @Key
        public int num_docks_available;

        @Key
        public int num_docks_disabled;

        @Key
        public int is_installed;

        @Key
        public int is_renting;

        @Key
        public int is_returning;
    }


    public static void main(final String[] args) throws Exception {
        // http GET citi bike station status
        HttpRequestFactory requestFactory =
                HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new JsonObjectParser(JSON_FACTORY));
                    }
                });
        CitiBikeURL url = new CitiBikeURL(stationInformationUrl);
        HttpRequest request = requestFactory.buildGetRequest(url);
        StationStatusFeed stationStatus = request.execute().parseAs(StationStatusFeed.class);
        if (stationStatus.data.stations.isEmpty()) {
            System.out.println("No station found.");
        } else {
            System.out.println(stationStatus.data.stations.size());
        }

        // public station status stream to Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // send events to kafka
        ObjectMapper mapper = new ObjectMapper();
        stationStatus.data.stations.forEach(st -> {
            try {
                String value = mapper.writeValueAsString(st);
                producer.send(new ProducerRecord<>(topicName, st.stationId, value));
            } catch (Exception e) {
                System.out.println("Failed to serialize");
                throw new RuntimeException(e.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }
}
