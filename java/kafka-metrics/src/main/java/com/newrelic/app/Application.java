package com.newrelic.app;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.kafkaclients.OpenTelemetryKafkaMetrics;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public class Application {

  private static final Logger LOGGER = LoggerFactory.getLogger(Application.class.getName());
  private static final List<String> TOPICS = List.of("foo", "bar", "baz");

  public static void main(String[] args) throws InterruptedException {
    OpenTelemetry openTelemetry = openTelemetry();
    OpenTelemetryKafkaMetrics.setOpenTelemetry(openTelemetry);

    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("com.newrelic.Kafka")))) {
      kafka.start();
      LOGGER.info("Starting up with bootstrap servers: {}" + kafka.getBootstrapServers());

      var producer = new Thread(new Producer(TOPICS, kafka.getBootstrapServers()));
      producer.start();

      var consumer = new Thread(new Consumer(TOPICS, kafka.getBootstrapServers()));
      consumer.start();

      producer.join();
      consumer.join();
    }
  }

  private static OpenTelemetry openTelemetry() {
    return AutoConfiguredOpenTelemetrySdk.builder()
        .addPropertiesSupplier(
            () ->
                Map.of(
                    "otel.service.name", "kafka-metrics",
                    "otel.traces.exporter", "none",
                    "otel.metric.export.interval", "5000"))
        .build()
        .getOpenTelemetrySdk();
  }
}
