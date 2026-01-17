package org.pipeline.consumers;

import org.models.LocationRecord;
import org.database.Redis.RedisCreate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;
import com.fasterxml.jackson.core.JsonProcessingException;

public class RedisConsumer {
    private Consumer<String, String> myConsumer;
    private ObjectMapper objectMapper;
    private RedisCreate redisRedisCreate;

    public RedisConsumer(){
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "air-quality-consumers");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.myConsumer = new KafkaConsumer<>(consumerConfig);
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.redisRedisCreate = new RedisCreate();

        myConsumer.subscribe(Collections.singletonList("air-quality-records"));
    }
    public void startConsuming(){
        System.out.println("🟢 Consumer is running and subscribed to air-quality-records topic");
        try{
            while(true){
                ConsumerRecords<String, String> records = myConsumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String record_value = record.value();
                        LocationRecord location_record = objectMapper.readValue(record_value, LocationRecord.class);
                        String recordJson = objectMapper.writeValueAsString(location_record);
                        redisRedisCreate.insertLastRecord(recordJson);
                        System.out.println("Processed record - Sensor ID: " + location_record.getSensorsId() +
                                         ", Location ID: " + location_record.getLocationsId() + 
                                         ", Value: " + location_record.getValue());
                    } catch (JsonProcessingException e) {
                        System.err.println("❌ Error processing record: " + e.getMessage());
                    }
                }
            }
        }catch(Exception e){
            System.err.println(e.getMessage());
        }finally {
            myConsumer.close();
        }
    }
}
