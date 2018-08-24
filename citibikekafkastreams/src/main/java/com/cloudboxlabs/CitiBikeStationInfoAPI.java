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


public class CitiBikeStationInfoAPI {
    private static final String stationInformationUrl =
            "https://gbfs.citibikenyc.com/gbfs/en/station_information.json";
    public static final String topicName = "station_information";

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

    /** Represents a StationInfo feed. */
    public static class StationInfoFeed {
        @Key("ttl")
        public int ttl;

        @Key("data")
        public StationInfoData data;
    }

    /** Represents a data object in response */
    public static class StationInfoData {
        @Key
        public List<StationInfo> stations;
    }

    /** Represents a station info object */
    public static class StationInfo {
        @Key("station_id")
        public String stationId;

        @Key
        public String name;

        @Key("lat")
        public float latitude;

        @Key("lon")
        public float longitude;

        @Key
        public int capacity;
    }

    public static void main(final String[] args) throws Exception {
        HttpRequestFactory requestFactory =
                HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new JsonObjectParser(JSON_FACTORY));
                    }
                });
        CitiBikeURL url = new CitiBikeURL(stationInformationUrl);
        HttpRequest request = requestFactory.buildGetRequest(url);
        StationInfoFeed stationInfo = request.execute().parseAs(StationInfoFeed.class);
        if (stationInfo.data.stations.isEmpty()) {
            System.out.println("No station found.");
        } else {
            System.out.println(stationInfo.data.stations.size());
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
        stationInfo.data.stations.forEach(st -> {
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
