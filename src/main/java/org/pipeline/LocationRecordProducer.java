package org.pipeline;

import org.apache.kafka.clients.producer.*;
import org.pipeline.models.LocationRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;

public class LocationRecordProducer {
    private Producer <String, String> myProducer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Fetcher fetcher = new Fetcher();
    public String all_records;
    public LocationRecordProducer(){
        Properties myProducerConfig = new Properties();

        myProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        myProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        myProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.myProducer = new KafkaProducer<>(myProducerConfig);
    }
    private void fetchAllRecords(){
        try{
            this.all_records = fetcher.fetchData(2178);
        }catch(Exception e){
            System.err.println(e.getMessage());
        }
    }
    public void sendRecords() {
        fetchAllRecords();
        if (all_records == null) {
            System.err.println("No records fetched");
            return;
        }try{
            JsonNode rootNode = objectMapper.readTree(all_records);
            JsonNode results = rootNode.get("results");

            if (results != null && results.isArray()) {
                for (JsonNode record : results) {
                    LocationRecord locationRecord = LocationRecord.builder()
                            .datetime(record.get("datetime").get("utc").asText())
                            .value((float) record.get("value").asDouble())
                            .sensorsId(record.get("sensorsId").asInt())
                            .locationsId(record.get("locationsId").asInt())
                            .latitude(record.get("coordinates").get("latitude").asDouble())
                            .longitude(record.get("coordinates").get("longitude").asDouble())
                            .build();

                    String recordJson = objectMapper.writeValueAsString(locationRecord);
                    ProducerRecord<String, String> a_record = new ProducerRecord<>("events", recordJson);
                    myProducer.send(a_record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e!=null){
                                System.out.println("❌ Adding event failed: " + e.getMessage());
                            }else{
                                System.out.println("✅ Event created " + recordJson);
                                System.out.println("✅ Event created to " + recordMetadata.topic() +
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
}
