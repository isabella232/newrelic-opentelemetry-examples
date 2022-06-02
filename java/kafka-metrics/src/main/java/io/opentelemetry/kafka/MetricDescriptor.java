package io.opentelemetry.kafka;

import com.google.auto.value.AutoValue;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.InstrumentValueType;

@AutoValue
abstract class MetricDescriptor {

  abstract String getName();

  abstract String getDescription();

  abstract String getUnit();

  abstract InstrumentType getInstrumentType();

  abstract InstrumentValueType getInstrumentValueType();

  static MetricDescriptor createDoubleGauge(String name, String description, String unit) {
    return new AutoValue_MetricDescriptor(
        name, description, unit, InstrumentType.OBSERVABLE_GAUGE, InstrumentValueType.DOUBLE);
  }
}
