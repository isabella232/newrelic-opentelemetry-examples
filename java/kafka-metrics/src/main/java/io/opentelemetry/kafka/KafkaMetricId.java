package io.opentelemetry.kafka;

import com.google.auto.value.AutoValue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.metrics.KafkaMetric;

@AutoValue
abstract class KafkaMetricId {

  abstract String getName();

  abstract String getGroup();

  abstract Set<String> getTagKeys();

  static KafkaMetricId create(KafkaMetric kafkaMetric) {
    return create(
        kafkaMetric.metricName().name(),
        kafkaMetric.metricName().group(),
        kafkaMetric.metricName().tags().keySet());
  }

  static KafkaMetricId create(
      String name, String group, Set<String> tagKeys) {
    return new AutoValue_KafkaMetricId(name, group, tagKeys);
  }
}
