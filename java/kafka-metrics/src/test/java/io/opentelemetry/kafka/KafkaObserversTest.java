package io.opentelemetry.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
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

class KafkaObserversTest {

  private static final Logger logger = Logger.getLogger(KafkaObservers.class.getName());

  @RegisterExtension static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  @RegisterExtension
  static OpenTelemetryExtension openTelemetryExtension = OpenTelemetryExtension.create();

  @Test
  void testProducerMetrics() throws InterruptedException {
    OpenTelemetrySdk openTelemetry = newRelicSdk();

    KafkaObservers.setOpenTelemetry(openTelemetry);

    final HashMap<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-client-id");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaObservers.class.getName());
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    Instant stopTime = Instant.now().plusSeconds(100);
    Random random = new Random();
    List<String> topics = List.of("foo", "bar", "baz", "qux");

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {

      while (Instant.now().isBefore(stopTime)) {
        producer.send(
            new ProducerRecord<>(
                topics.get(random.nextInt(topics.size())),
                0,
                System.currentTimeMillis(),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)));
        Thread.sleep(random.nextInt(100));
      }

      openTelemetry.getSdkMeterProvider().forceFlush().join(10, TimeUnit.SECONDS);
      openTelemetry.getSdkMeterProvider().shutdown().join(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void testConsumerMetrics() throws InterruptedException {
    OpenTelemetrySdk openTelemetry = newRelicSdk();

    KafkaObservers.setOpenTelemetry(openTelemetry);

    writeDummyRecords("sample_topic", 1000);

    final HashMap<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaObservers.class.getName());
    consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

    Instant stopTime = Instant.now().plusSeconds(100);

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
      consumer.subscribe(Collections.singletonList("sample_topic"));
      while (Instant.now().isBefore(stopTime)) {
        consumer.poll(Duration.ofSeconds(1));
      }
      Thread.sleep(100);
    }

    openTelemetry.getSdkMeterProvider().forceFlush().join(10, TimeUnit.SECONDS);
    openTelemetry.getSdkMeterProvider().shutdown().join(10, TimeUnit.SECONDS);
  }

  private OpenTelemetrySdk newRelicSdk() {
    return
        OpenTelemetrySdk.builder()
            .setMeterProvider(
                SdkMeterProvider.builder()
                    .setResource(
                        Resource.getDefault().toBuilder().put("service.name", "kafka-test").build())
                    .registerMetricReader(
                        PeriodicMetricReader.builder(
                                OtlpGrpcMetricExporter.builder()
                                    .setAggregationTemporalitySelector(
                                        AggregationTemporalitySelector.deltaPreferred())
                                    .setEndpoint("https://otlp.nr-data.net:4317")
                                    .addHeader("api-key", "NRII-zrwWPvnDYVljVmIpOHRdUOXPpbrspww2")
                                    .build())
                            .setInterval(Duration.ofSeconds(1))
                            .build())
                    .build())
            .build();
  }

  private void writeDummyRecords(String topic, int numberOfRecords) {
    final HashMap<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-client-id");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaObservers.class.getName());
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {

      for (int i = 0; i < numberOfRecords; i++) {
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
