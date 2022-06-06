# Kafka Metrics Demo

## Introduction

This project demonstrates a simple Java application which produces and consumes from kafka, and bridges kafka client metrics to OpenTelemetry. It spins up kafka using docker and [Kafka test containers](https://www.testcontainers.org/modules/kafka/).

## Run

Set the following environment variables:
* `OTEL_EXPORTER_OTLP_HEADERS=api-key=<your_license_key>>`
  * Replace `<your_license_key>` with your [Account License Key](https://one.newrelic.com/launcher/api-keys-ui.launcher).
* `OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.nr-data.net:4317`
  * Configure the application to export to New Relic via OTLP.

Run the application from a shell in the [java root](../) via:
```
./gradlew :kafka-metrics:run
```

The application sets up a resource with `service.name=kafka-metrics`.

Check New Relic to confirm data is flowing.