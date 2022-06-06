package com.newrelic.app;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public class Application {

  private static final Logger LOGGER = LoggerFactory.getLogger(Application.class.getName());
  private static final Random RANDOM = new Random();
  private static final List<String> TOPICS = List.of("foo", "bar", "baz");

  public static void main(String[] args) throws InterruptedException {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("com.newrelic.Kafka")))) {
      kafka.start();
      LOGGER.info("Starting up with bootstrap servers: {}" + kafka.getBootstrapServers());

      var producer = new Thread(new Producer(kafka.getBootstrapServers()));
      producer.start();

      var consumer = new Thread(new Consumer(kafka.getBootstrapServers()));
      consumer.start();

      producer.join();
      consumer.join();
    }
  }

  static class Producer implements Runnable {

    private final String bootstrapServers;

    private Producer(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void run() {
      Map<String, Object> config = new HashMap<>();
      config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      config.put(ProducerConfig.CLIENT_ID_CONFIG, "dummy-client-id");
      config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

      var counter = new AtomicLong();

      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
        while (true) {
          if (Thread.interrupted()) {
            return;
          }
          try {
            Thread.sleep(RANDOM.nextInt(2000));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          String topic = TOPICS.get(RANDOM.nextInt(TOPICS.size()));
          LOGGER.info("Producing message {} to topic {}", counter.incrementAndGet(), topic);
          producer.send(
              new ProducerRecord<>(
                  topic,
                  0,
                  System.currentTimeMillis(),
                  "key".getBytes(StandardCharsets.UTF_8),
                  "value".getBytes(StandardCharsets.UTF_8)));
        }
      }
    }
  }

  static class Consumer implements Runnable {

    private final String bootstrapServers;

    private Consumer(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void run() {
      Map<String, Object> config = new HashMap<>();
      config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      config.put(ConsumerConfig.GROUP_ID_CONFIG, "dummy-group-id");
      config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
      config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

      var counter = new AtomicLong();

      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
        consumer.subscribe(TOPICS);
        while (true) {
          if (Thread.interrupted()) {
            return;
          }
          ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
          if (records.isEmpty()) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          records.forEach(
              consumerRecord ->
                  LOGGER.info(
                      "Consuming message {} from topic {}, partition {}, key {}: {}",
                      counter.incrementAndGet(),
                      consumerRecord.topic(),
                      consumerRecord.partition(),
                      new String(consumerRecord.key()),
                      new String(consumerRecord.value())));
          consumer.commitSync();
        }
      }
    }
  }
}
