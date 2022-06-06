package com.newrelic.app;

import com.github.javafaker.Faker;
import io.opentelemetry.instrumentation.kafkaclients.OpenTelemetryKafkaMetrics;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Producer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class.getName());
  private static final Random RANDOM = new Random();
  private static final Faker FAKER = new Faker();

  private final List<String> topics;
  private final String bootstrapServers;

  Producer(List<String> topics, String bootstrapServers) {
    this.topics = topics;
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
    config.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, OpenTelemetryKafkaMetrics.class.getName());

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
        String topic = topics.get(RANDOM.nextInt(topics.size()));
        String key = UUID.randomUUID().toString();
        LOGGER.info("Producing message {} to topic {}, key {}", counter.incrementAndGet(), topic, key);
        producer.send(
            new ProducerRecord<>(
                topic,
                0,
                System.currentTimeMillis(),
                key.getBytes(StandardCharsets.UTF_8),
                FAKER.lorem().sentence().getBytes(StandardCharsets.UTF_8)));
      }
    }
  }
}
