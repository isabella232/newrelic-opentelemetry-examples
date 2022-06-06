package io.opentelemetry.kafka;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class OpenTelemetryKafkaMetricsTest {

  private static final List<String> TOPICS = Arrays.asList("foo", "bar", "baz", "qux");
  private static final Random RANDOM = new Random();

  private static final Logger logger = Logger.getLogger(OpenTelemetryKafkaMetrics.class.getName());

  @RegisterExtension static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  @RegisterExtension
  static OpenTelemetryExtension openTelemetryExtension = OpenTelemetryExtension.create();

  @Test
  void observeMetrics() {
    OpenTelemetryKafkaMetrics.setOpenTelemetry(openTelemetryExtension.getOpenTelemetry());

    produceRecords();
    consumeRecords();

    Set<String> metricNames =
        openTelemetryExtension.getMetrics().stream().map(MetricData::getName).collect(toSet());

    KafkaMetricRegistry.getMetricDescriptors()
        .forEach(metricDescriptor -> assertTrue(metricNames.contains(metricDescriptor.getName())));

    // Print mapping table
    OpenTelemetryKafkaMetrics.printMappingTable();
  }

  void produceRecords() {
    Map<String, Object> config = new HashMap<>();
    // Register OpenTelemetryKafkaMetrics as reporter
    config.put(
        ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, OpenTelemetryKafkaMetrics.class.getName());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-client-id");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < 100; i++) {
        producer.send(
            new ProducerRecord<>(
                TOPICS.get(RANDOM.nextInt(TOPICS.size())),
                0,
                System.currentTimeMillis(),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)));
      }
    }
  }

  void consumeRecords() {
    Map<String, Object> config = new HashMap<>();
    // Register OpenTelemetryKafkaMetrics as reporter
    config.put(
        ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OpenTelemetryKafkaMetrics.class.getName());
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    config.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(TOPICS);
      Instant stopTime = Instant.now().plusSeconds(10);
      while (Instant.now().isBefore(stopTime)) {
        consumer.poll(Duration.ofSeconds(1));
      }
    }
  }
}
