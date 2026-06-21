# Runlet

Runlet is an early JVM library for small, embeddable, batch-oriented stream
processing pipelines.

It is meant for jobs that need more structure than hand-written loops or
`Flow`, but do not justify running Flink, Kafka Streams, or Spark Streaming.
Runlet runs inside your process: no broker, no cluster, no daemon.

## Status

Runlet is pre-release and the API will change.

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

Modules:

- `runlet-core`: core API, DSL, runtime, and blocking adapters
- `runlet-connector-file`: file source, file checkpoint store, and chunk-file sink
- `runlet-adapter-spring`: optional Spring `SmartLifecycle` integration

Not implemented yet:

- windowing or `groupBy`
- event-time semantics or watermarks
- exactly-once semantics
- distributed execution
- built-in JSON serialization
- Spring Boot autoconfiguration

For more detail:

- [Design notes](docs/design.md)
- [Failure semantics](docs/failure-semantics.md)

## Install

Runlet is not published yet. Intended coordinates:

```kotlin
dependencies {
    implementation("org.aetherlink:runlet-core:1.0-SNAPSHOT")
    implementation("org.aetherlink:runlet-connector-file:1.0-SNAPSHOT")
    implementation("org.aetherlink:runlet-adapter-spring:1.0-SNAPSHOT")
}
```

For now, build from source:

```bash
./gradlew check
./gradlew :runlet-core:jar
./gradlew :runlet-connector-file:jar
./gradlew :runlet-adapter-spring:jar
./gradlew publishToMavenLocal
```

## Checkpointed File Pipeline

This example reads lines from a file, keeps completed records, transforms them,
and writes replay-safe chunk files.

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
`.checkpoint(...)` has been called. That is enforced by the DSL types rather
than a runtime capability check.

## Typed JSON Lines

Runlet does not include a JSON library yet. Use your serializer of choice and
pass decode/encode functions explicitly:

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

## Uncheckpointed Pipelines

Uncheckpointed pipelines run stages concurrently with bounded channels between
the source, stages, and sink.

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

## Spring Lifecycle Adapter

Spring applications can wrap a pipeline as a `SmartLifecycle` bean:

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

Spring Boot autoconfiguration is not implemented yet.

## Development

Run the full verification suite:

```bash
./gradlew check
```

This runs compilation, tests, and ktlint.

Useful tasks:

```bash
./gradlew test
./gradlew :runlet-core:test
./gradlew :runlet-connector-file:test
./gradlew :runlet-adapter-spring:test
./gradlew ktlintCheck
./gradlew ktlintFormat
```

## Non-Goals

If you need event-time correctness, exactly-once distributed processing, or
horizontal scale, use Flink, Kafka Streams, or Spark Streaming. Runlet is for
small, local, embeddable JVM pipelines.
