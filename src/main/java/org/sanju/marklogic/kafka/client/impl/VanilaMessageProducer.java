package org.sanju.marklogic.kafka.client.impl;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.sanju.marklogic.kafka.client.beans.MarkLogicDocument.DocumentType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class VanilaMessageProducer {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
        final ObjectNode objectNode = MAPPER.createObjectNode();
        objectNode.put("name", "Doe, John");
        objectNode.put("phone", "111-111-1111");
        
        //send a json document
        objectNode.put("url", "/my-sweet-trade.json");
        objectNode.put("type", DocumentType.JSON.getV());
        producer.send(new ProducerRecord<String, JsonNode>(topicName, objectNode));        
        
        //send an xml document
        objectNode.put("url", "/my-sweet-trade.xml");
        objectNode.put("type", DocumentType.XML.getV());
        producer.send(new ProducerRecord<String, JsonNode>(topicName, objectNode));   
        
        producer.flush();
        producer.close();
        System.out.println("Message sent successfully");
    }
}
