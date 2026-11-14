package dev.tjb.runlet.sample.spring.boot

import com.fasterxml.jackson.databind.ObjectMapper
import dev.tjb.runlet.adapter.spring.boot.RunletPipelineRegistration
import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.Sink
import dev.tjb.runlet.connector.file.FileCheckpointStore
import dev.tjb.runlet.connector.jackson.JacksonChunkFileSink
import dev.tjb.runlet.connector.jackson.JacksonFileSource
import dev.tjb.runlet.connector.jackson.defaultRunletObjectMapper
import dev.tjb.runlet.dsl.Runlet
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.writeLines

@SpringBootApplication(proxyBeanMethods = false)
class RunletSampleApplication

fun main(args: Array<String>) {
    runApplication<RunletSampleApplication>(*args)
}

@Configuration(proxyBeanMethods = false)
class OrdersPipelineConfiguration {
    @Bean
    fun objectMapper(): ObjectMapper = defaultRunletObjectMapper()

    @Bean
    fun orderSource(
        @Value("\${sample.orders.input}") input: Path,
        @Value("\${sample.orders.chunk-size}") chunkSize: Int,
        objectMapper: ObjectMapper,
    ): CheckpointableSource<OrderEvent> {
        seedInputFile(input)
        return JacksonFileSource.jsonLines(
            path = input,
            type = OrderEvent::class.java,
            chunkSize = chunkSize,
            objectMapper = objectMapper,
        )
    }

    @Bean
    fun orderCheckpointStore(
        @Value("\${sample.orders.checkpoint}") checkpoint: Path,
    ): CheckpointStore = FileCheckpointStore(checkpoint)

    @Bean
    fun orderSummarySink(
        @Value("\${sample.orders.output}") output: Path,
        objectMapper: ObjectMapper,
    ): Sink<OrderSummary> =
        JacksonChunkFileSink.jsonLines(
            directory = output,
            objectMapper = objectMapper,
        )

    @Bean
    fun ordersPipeline(
        runletRuntimeConfig: RunletRuntimeConfig,
        orderSource: CheckpointableSource<OrderEvent>,
        orderCheckpointStore: CheckpointStore,
        orderSummarySink: Sink<OrderSummary>,
    ): RunletPipelineRegistration =
        RunletPipelineRegistration("orders") {
            Runlet("orders", config = runletRuntimeConfig) {
                source(orderSource)
                    .checkpoint(orderCheckpointStore)
                    .filter { order -> order.status == "completed" }
                    .map { order -> order.toSummary() }
                    .sink(orderSummarySink)
            }
        }

    private fun seedInputFile(input: Path) {
        if (input.exists()) return

        input.parent?.createDirectories()
        input.writeLines(
            listOf(
                """{"id":"order-1001","status":"completed","totalCents":4299}""",
                """{"id":"order-1002","status":"pending","totalCents":1299}""",
                """{"id":"order-1003","status":"completed","totalCents":9999}""",
            ),
        )
    }
}

data class OrderEvent(
    val id: String,
    val status: String,
    val totalCents: Long,
)

data class OrderSummary(
    val id: String,
    val totalCents: Long,
)

private fun OrderEvent.toSummary(): OrderSummary =
    OrderSummary(
        id = id,
        totalCents = totalCents,
    )
