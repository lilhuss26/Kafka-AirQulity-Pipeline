package org.pipeline;

import org.pipeline.models.LocationRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import com.fasterxml.jackson.core.JsonProcessingException;

public class InfluxConsumer {
    private Consumer<String, String> myConsumer;
    private ObjectMapper objectMapper;

    public InfluxConsumer() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "air-quality-consumers");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.myConsumer = new KafkaConsumer<>(consumerConfig);
        this.objectMapper = new ObjectMapper();

        myConsumer.subscribe(Collections.singletonList("air-quality-records"));
    }
    public void startConsuming(){
        System.out.println("🟢 Consumer is running and subscribed to air-quality-records topic");
        try{
            while(true){
                ConsumerRecords<String, String> records = myConsumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    continue;
                }for (ConsumerRecord<String, String> record : records) {
                    try {
                        String record_value = record.value();
                        LocationRecord location_record = objectMapper.readValue(record_value, LocationRecord.class);

                    } catch (JsonProcessingException e) {
                        System.out.println("❌ Error: " + e.getMessage());
                    }
                }

            }
        }catch(Exception e){
            System.err.println(e.getMessage());
        }
    }
}
