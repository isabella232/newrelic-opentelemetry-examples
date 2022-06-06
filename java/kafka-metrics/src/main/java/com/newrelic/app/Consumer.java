package com.newrelic.app;

import io.opentelemetry.instrumentation.kafkaclients.OpenTelemetryKafkaMetrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Consumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class.getName());

  private final List<String> topics;
  private final String bootstrapServers;

  Consumer(List<String> topics, String bootstrapServers) {
    this.topics = topics;
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void run() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "dummy-group-id");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OpenTelemetryKafkaMetrics.class.getName());

    var counter = new AtomicLong();

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(topics);
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
