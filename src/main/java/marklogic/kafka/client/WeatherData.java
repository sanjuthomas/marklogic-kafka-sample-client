package marklogic.kafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

public class WeatherData {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final Producer<String, JsonNode> producer;
  private static final String topicName = "open_weather_data";

  static {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 3);
    props.put("batch.size", 100);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 2048);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
    producer = new KafkaProducer<String, JsonNode>(props);
  }

  public static void main(String[] args) {

    Flux.interval(Duration.ofSeconds(10))
      .doOnNext(r -> {
        WebClient.builder()
          .baseUrl("http://api.openweathermap.org/data/2.5/weather?q=Bangalore&appid={key}")
          .build()
          .get()
          .retrieve()
          .bodyToMono(Map.class)
          .doOnNext(result -> sendToKafka(result))
          .subscribe();
      }).blockLast();
  }

  static void sendToKafka(Map data) {
    final JsonNode jsonNode = JSON_MAPPER.valueToTree(data);
    producer.send(new ProducerRecord<String, JsonNode>(topicName, jsonNode));
  }

}
