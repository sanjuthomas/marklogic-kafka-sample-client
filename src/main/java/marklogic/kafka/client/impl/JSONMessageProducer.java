package marklogic.kafka.client.impl;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import marklogic.kafka.client.beans.Account;
import marklogic.kafka.client.beans.Client;
import marklogic.kafka.client.beans.QuoteRequest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class JSONMessageProducer {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {
      
        String [] symbols = new String [] {"ABB", "AAV", "AAPL", "BABA", "BAK", "BANC", "DAL", "DBD", "DBL", "RACE", "RATE", "RCG", "YELP", "YUME", "ZPIN"};

        String topicName = "quote_request";
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
        for (int i = 0; i < 1000; i++){
            Thread.sleep(10000);
            final Account account = new Account("A" + i);
            final Client client = new Client("C" + i, account);
            final QuoteRequest quoteRequest = new QuoteRequest("Q" + i, symbols[ThreadLocalRandom.current().nextInt(0, 14)], 
                    ThreadLocalRandom.current().nextInt(1, 100 + 1), client, new Date());
            final JsonNode jsonNode = MAPPER.valueToTree(quoteRequest);
            producer.send(new ProducerRecord<String, JsonNode>(topicName, jsonNode));
        }
        producer.flush();
        producer.close();
        System.out.println("Message sent successfully");
    }
}
