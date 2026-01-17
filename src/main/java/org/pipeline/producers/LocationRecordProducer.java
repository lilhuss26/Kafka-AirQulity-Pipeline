package org.pipeline.producers;

import org.apache.kafka.clients.producer.*;
import org.models.LocationRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;

public class LocationRecordProducer {
    private Producer<String, String> myProducer;
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private Fetcher fetcher = new Fetcher();
    private ScheduledExecutorService scheduler;

    public LocationRecordProducer() {
        Properties myProducerConfig = new Properties();

        myProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        myProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        myProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.myProducer = new KafkaProducer<>(myProducerConfig);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    private void fetchAllRecords() {
        try {
            String all_records = fetcher.fetchData(2178);
            processRecords(all_records);
        } catch (Exception e) {
            System.err.println("Error fetching records: " + e.getMessage());
        }
    }

    private void processRecords(String all_records) {
        if (all_records == null) {
            System.err.println("No records fetched");
            return;
        }

        try {
            JsonNode rootNode = objectMapper.readTree(all_records);
            JsonNode results = rootNode.get("results");

            if (results != null && results.isArray()) {
                for (JsonNode record : results) {
                    LocationRecord locationRecord = LocationRecord.builder()
                            .datetime(Instant.parse(record.get("datetime").get("utc").asText()))
                            .value((float) record.get("value").asDouble())
                            .sensorsId(record.get("sensorsId").asInt())
                            .locationsId(record.get("locationsId").asInt())
                            .latitude(record.get("coordinates").get("latitude").asDouble())
                            .longitude(record.get("coordinates").get("longitude").asDouble())
                            .build();

                    String recordJson = objectMapper.writeValueAsString(locationRecord);
                    ProducerRecord<String, String> a_record = new ProducerRecord<>("air-quality-records", recordJson);

                    myProducer.send(a_record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                System.out.println("❌ Adding record failed: " + e.getMessage());
                            } else {
                                System.out.println("✅ Record created " + recordJson);
                                System.out.println("✅ Record created to " + recordMetadata.topic() +
                                        " : partition " + recordMetadata.partition() +
                                        " : at offset " + recordMetadata.offset());
                            }
                        }
                    });
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing records: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void startPeriodicFetching() {
        scheduler.scheduleAtFixedRate(
                this::fetchAllRecords,
                0,
                3,
                TimeUnit.SECONDS
        );

        System.out.println("Started periodic fetching every 3 seconds...");
    }

    public void shutdown() {
        System.out.println("Shutting down...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        myProducer.close();
    }
}