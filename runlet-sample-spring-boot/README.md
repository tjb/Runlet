# Runlet Spring Boot Sample

This module is a small Spring Boot application that shows how to wire Runlet
through the starter.

It demonstrates:

- `RunletPipelineRegistration` as the Spring bean that registers a pipeline
- starter-managed `RunletRuntimeConfig`
- `application.yml` configuration for Runlet worker threads and runtime buffers
- an application-owned `CheckpointStore`
- Jackson JSON Lines source and sink connectors
- optional Micrometer metrics through Spring Boot Actuator

Run it with:

```bash
./gradlew :runlet-sample-spring-boot:run
```

The sample seeds a small input file if one does not exist:

```text
build/runlet-sample/input/orders.jsonl
```

It writes checkpoint state and output chunks under:

```text
build/runlet-sample/state/orders.ckpt
build/runlet-sample/output/order-summaries/
```

The interesting code is in
`src/main/kotlin/org/aetherlink/runlet/sample/spring/boot/RunletSampleApplication.kt`.
