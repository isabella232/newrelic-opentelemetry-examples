package io.opentelemetry.kafka;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.InstrumentValueType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

public class KafkaObservers implements MetricsReporter {

  private static final Logger logger = Logger.getLogger(KafkaObservers.class.getName());

  @Nullable private static Meter meter;

  private final Map<RegisteredInstrument, AutoCloseable> instrumentMap = new ConcurrentHashMap<>();

  public static void setOpenTelemetry(OpenTelemetry openTelemetry) {
    meter = openTelemetry.getMeter("kafka-observers");
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    for (KafkaMetric metric : metrics) {
      metricChange(metric);
    }
  }

  @Override
  public void metricChange(KafkaMetric metric) {
    KafkaMetricId kafkaMetricId = KafkaMetricId.create(metric);
    Meter currentMeter;
    if (meter == null) {
      logger.log(Level.FINEST, "Metric changed but meter not set: " + kafkaMetricId);
      return;
    }
    currentMeter = meter;

    MetricDescriptor metricDescriptor = KafkaMetricRegistry.getRegisteredInstrument(kafkaMetricId);
    if (metricDescriptor == null) {
      logger.log(
          Level.FINEST,
          "Metric changed but did not match any metrics from registry: " + kafkaMetricId);
      return;
    }

    AttributesBuilder attributesBuilder = Attributes.builder();
    metric.metricName().tags().forEach(attributesBuilder::put);
    RegisteredInstrument registeredInstrument =
        RegisteredInstrument.create(kafkaMetricId, attributesBuilder.build());

    instrumentMap.compute(
        registeredInstrument,
        (registeredInstrument1, autoCloseable) -> {
          if (autoCloseable != null) {
            logger.log(Level.FINEST, "Replacing instrument " + registeredInstrument1);
            try {
              autoCloseable.close();
            } catch (Exception e) {
              logger.log(Level.WARNING, "An error occurred closing instrument", e);
            }
          } else {
            logger.log(Level.FINEST, "Adding instrument " + registeredInstrument1);
          }
          return createObservable(currentMeter, registeredInstrument1, metricDescriptor, metric);
        });
  }

  private static AutoCloseable createObservable(
      Meter meter,
      RegisteredInstrument registeredInstrument,
      MetricDescriptor metricDescriptor,
      KafkaMetric kafkaMetric) {
    if (metricDescriptor.getInstrumentType() == InstrumentType.OBSERVABLE_GAUGE
        && metricDescriptor.getInstrumentValueType() == InstrumentValueType.DOUBLE) {
      return meter
          .gaugeBuilder(metricDescriptor.getName())
          .setDescription(metricDescriptor.getDescription())
          .setUnit(metricDescriptor.getUnit())
          .buildWithCallback(
              observableMeasurement ->
                  observableMeasurement.record(
                      (Double) kafkaMetric.metricValue(), registeredInstrument.getAttributes()));
    }
    throw new IllegalStateException("Unsupported metric descriptor: " + metricDescriptor);
  }

  private static Attributes toAttributes(KafkaMetric kafkaMetric) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    kafkaMetric.metricName().tags().forEach(attributesBuilder::put);
    return attributesBuilder.build();
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    KafkaMetricId kafkaMetricId = KafkaMetricId.create(metric);
    logger.log(Level.INFO, "metricRemoval: " + kafkaMetricId);
    instrumentMap.remove(RegisteredInstrument.create(kafkaMetricId, toAttributes(metric)));
  }

  @Override
  public void close() {
    logger.log(Level.INFO, "close");
  }

  @Override
  public void configure(Map<String, ?> configs) {
    logger.log(Level.INFO, "configure: " + configs);
  }
}
