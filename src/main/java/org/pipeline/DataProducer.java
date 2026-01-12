package org.pipeline;

//import com.kafka.models.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class DataProducer {
    private Producer <String, String> myProducer;
    private ObjectMapper objectMapper = new ObjectMapper();

    public DataProducer(){
        Properties myProducerConfig = new Properties();

        myProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        myProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        myProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.myProducer = new KafkaProducer<>(myProducerConfig);
    }

}
