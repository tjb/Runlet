# Runlet

Runlet is a small JVM library for embeddable, batch-oriented stream processing
pipelines.

It is for jobs that need more structure than hand-written loops or `Flow`, but
do not justify operating Flink, Kafka Streams, or Spark Streaming. Runlet runs
inside your process: no broker, no cluster, no daemon.

## Status

Runlet is pre-release. APIs, module names, and behavior may change before a
stable release.

Current v0 scope:

- single JVM process
- one source, one linear pipeline, one sink
- chunked execution with `Chunk<T>`
- `map`, `filter`, and `evalMap`
- bounded channels for uncheckpointed pipelines
- serial checkpointed execution for ordered, resumable sources
- file line source, file checkpoint store, and chunk-file sink
- blocking adapters for Java and other blocking JVM integrations
- Spring `SmartLifecycle` adapter
- Spring Boot starter and autoconfiguration

Not implemented yet:

- windowing or `groupBy`
- event-time semantics or watermarks
- exactly-once semantics
- distributed execution
- built-in JSON serialization
- Actuator health or metrics integration

## Modules

| Module | Purpose |
| --- | --- |
| `runlet-core` | Core API, DSL, runtime, and blocking adapters. |
| `runlet-connector-file` | File source, file checkpoint store, and chunk-file sink. |
| `runlet-adapter-spring` | Spring Framework `SmartLifecycle` integration. |
| `runlet-spring-boot-autoconfigure` | Spring Boot autoconfiguration. |
| `runlet-spring-boot-starter` | Convenience dependency for Spring Boot applications. |

## Install

Runlet is not published to a remote Maven repository yet. For local use, publish
the artifacts to your Maven local repository:

```bash
./gradlew check
./gradlew publishToMavenLocal
```

Then add `mavenLocal()` and the modules you need:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.aetherlink:runlet-core:1.0-SNAPSHOT")
    implementation("org.aetherlink:runlet-connector-file:1.0-SNAPSHOT")
}
```

For Spring Boot applications, prefer the starter:

```kotlin
dependencies {
    implementation("org.aetherlink:runlet-spring-boot-starter:1.0-SNAPSHOT")
    implementation("org.aetherlink:runlet-connector-file:1.0-SNAPSHOT")
}
```

## Quick Start

This checkpointed file pipeline reads lines from a file, keeps completed
records, transforms them, and writes replay-safe chunk files.

```kotlin
import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.connector.file.ChunkFileSink
import org.aetherlink.runlet.connector.file.FileCheckpointStore
import org.aetherlink.runlet.connector.file.FileSource
import org.aetherlink.runlet.dsl.Runlet

fun main() = runBlocking {
    Runlet("orders") {
        source(FileSource.lines("orders.txt", chunkSize = 1024))
            .checkpoint(FileCheckpointStore("orders.ckpt"))
            .filter { line -> line.contains("completed") }
            .map { line -> line.uppercase() }
            .sink(ChunkFileSink.lines("summaries"))
    }.run()
}
```

Checkpointed pipelines run one chunk at a time:

```text
read -> transform -> write -> commit -> persist cursor
```

The checkpoint cursor only advances after the sink commit returns. If `write()`
or `commit()` fails, the checkpoint does not advance.

For checkpointable sources, `.sink(...)` is only available after
`.checkpoint(...)` has been called. The DSL enforces this with types rather than
a runtime capability check.

## Spring Boot

Spring Boot applications can register Runlet pipelines as beans. The starter
creates a shared coroutine scope, wraps each registration in a `SmartLifecycle`,
and starts/stops pipelines with the application context.

```kotlin
import org.aetherlink.runlet.adapter.spring.boot.RunletPipelineRegistration
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.dsl.Runlet
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class PipelineConfiguration {
    @Bean
    fun ordersPipeline(runletRuntimeConfig: RunletRuntimeConfig): RunletPipelineRegistration =
        RunletPipelineRegistration("orders") {
            Runlet("orders", config = runletRuntimeConfig) {
                source(orderSource)
                    .checkpoint(orderCheckpointStore)
                    .map(::summarize)
                    .sink(orderSink)
            }
        }
}
```

`application.yml`:

```yaml
runlet:
  enabled: true
  threads: 4
  shutdown-timeout: 30s
  runtime:
    channel-capacity: 4
```

Connector-specific settings, such as `FileSource.lines(..., chunkSize = 1024)`,
are still chosen when constructing that source.

## Runtime Model

Runlet moves records through a pipeline as chunks, not one record at a time.
Stages still use ordinary per-record functions, but the runtime batches the
plumbing around them.

Uncheckpointed pipelines run stages concurrently with bounded channels between
the source, stages, and sink:

```kotlin
import org.aetherlink.runlet.api.RunletRuntimeConfig

Runlet(
    name = "fast-path",
    config = RunletRuntimeConfig(channelCapacity = 4),
) {
    source(mySource)
        .map(::normalize)
        .evalMap(::enrich)
        .sink(mySink)
}.run()
```

Checkpointed pipelines intentionally stay serial in v0 because cursor
advancement depends on sink durability.

## JSON Lines

Runlet does not include a JSON library. Use your serializer of choice and pass
decode/encode functions explicitly:

```kotlin
val source = FileSource.jsonLines(
    path = "orders.jsonl",
    decode = ::decodeOrder,
)

val sink = ChunkFileSink.jsonLines(
    directory = "summaries",
    encode = ::encodeSummary,
)
```

## Blocking Adapters

Java and blocking JVM integrations can implement blocking interfaces and adapt
them into Runlet's coroutine contracts:

```kotlin
import org.aetherlink.runlet.adapter.blocking.BlockingSink
import org.aetherlink.runlet.adapter.blocking.asSink
import org.aetherlink.runlet.api.Chunk

class ConsoleBlockingSink : BlockingSink<String> {
    override fun write(chunk: Chunk<String>) {
        chunk.records.forEach(::println)
    }
}

val sink = ConsoleBlockingSink().asSink()
```

Blocking adapter calls run on `Dispatchers.IO`.

## Spring Framework

Applications that use Spring Framework without Spring Boot can wrap a pipeline
as a `SmartLifecycle` bean:

```kotlin
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import org.aetherlink.runlet.adapter.spring.SpringPipelineLifecycle
import java.util.concurrent.Executors

val dispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()
val scope = CoroutineScope(SupervisorJob() + dispatcher)

val lifecycle = SpringPipelineLifecycle(
    pipeline = pipeline,
    scope = scope,
    onFailure = { failure -> logger.error("Runlet pipeline failed", failure) },
)
```

## Development

Run the full verification suite:

```bash
./gradlew check
```

This runs compilation, tests, and ktlint.

Useful tasks:

```bash
./gradlew test
./gradlew ktlintCheck
./gradlew ktlintFormat
./gradlew publishToMavenLocal
```

## Design Notes

- [Design notes](docs/design.md)
- [Failure semantics](docs/failure-semantics.md)

## Non-Goals

If you need event-time correctness, exactly-once distributed processing, or
horizontal scale, use Flink, Kafka Streams, or Spark Streaming. Runlet is for
small, local, embeddable JVM pipelines.
