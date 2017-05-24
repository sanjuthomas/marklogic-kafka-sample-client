package marklogic.kafka.client.impl;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class VanilaMessageProducer {
    
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
      
        String topicName = "trades";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 100);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 2048);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        Producer<String, ObjectNode> producer = new KafkaProducer<String, ObjectNode>(props);
        final ObjectNode json = JSON_MAPPER.createObjectNode();
        json.put("name", "Doe, John");
        json.put("phone", "111-111-1111");
        json.put("url", "/my-sweet-trade.json");
        producer.send(new ProducerRecord<String, ObjectNode>(topicName, json));     
        producer.flush();
        producer.close();
        System.out.println("Message sent successfully");
    }
}
