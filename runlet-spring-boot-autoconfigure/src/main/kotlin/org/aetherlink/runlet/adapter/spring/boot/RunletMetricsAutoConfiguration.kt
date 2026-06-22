package org.aetherlink.runlet.adapter.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import org.aetherlink.runlet.api.PipelineObserver
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@AutoConfiguration(after = [RunletAutoConfiguration::class])
@ConditionalOnClass(MeterRegistry::class)
@ConditionalOnBean(MeterRegistry::class)
@ConditionalOnProperty(prefix = "runlet.metrics", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class RunletMetricsAutoConfiguration {
    @Bean(name = ["runletMetricsObserver"])
    @ConditionalOnMissingBean(name = ["runletMetricsObserver"])
    fun runletMetricsObserver(meterRegistry: MeterRegistry): PipelineObserver =
        MicrometerRunletMetrics(
            meterRegistry = meterRegistry,
            clock = Clock.systemUTC(),
        )
}

class MicrometerRunletMetrics(
    private val meterRegistry: MeterRegistry,
    private val clock: Clock,
) : PipelineObserver {
    private val running = ConcurrentHashMap<String, AtomicInteger>()
    private val lastSuccessEpochSeconds = ConcurrentHashMap<String, AtomicLong>()
    private val lastFailureEpochSeconds = ConcurrentHashMap<String, AtomicLong>()

    override fun onPipelineStarted(name: String) {
        state(name).running.set(1)
        meterRegistry.counter("runlet.pipeline.starts", "pipeline", name).increment()
    }

    override fun onPipelineCompleted(name: String) {
        val state = state(name)
        state.running.set(0)
        state.lastSuccessEpochSeconds.set(clock.instant().epochSecond)
        meterRegistry.counter("runlet.pipeline.completions", "pipeline", name).increment()
    }

    override fun onPipelineStopped(name: String) {
        state(name).running.set(0)
    }

    override fun onPipelineFailed(
        name: String,
        failure: Throwable,
    ) {
        val state = state(name)
        state.running.set(0)
        state.lastFailureEpochSeconds.set(clock.instant().epochSecond)
        meterRegistry
            .counter("runlet.pipeline.failures", "pipeline", name, "exception", failure::class.java.simpleName)
            .increment()
    }

    override fun onChunkCommitted(
        name: String,
        records: Int,
    ) {
        state(name)
        meterRegistry.counter("runlet.pipeline.chunks", "pipeline", name).increment()
        meterRegistry.counter("runlet.pipeline.records", "pipeline", name).increment(records.toDouble())
    }

    private fun state(name: String): PipelineMetricState =
        PipelineMetricState(
            running = running.computeIfAbsent(name) { key -> registerRunningGauge(key) },
            lastSuccessEpochSeconds = lastSuccessEpochSeconds.computeIfAbsent(name) { key -> registerLastSuccessGauge(key) },
            lastFailureEpochSeconds = lastFailureEpochSeconds.computeIfAbsent(name) { key -> registerLastFailureGauge(key) },
        )

    private fun registerRunningGauge(name: String): AtomicInteger {
        val value = AtomicInteger(0)
        meterRegistry.gauge("runlet.pipeline.running", listOf(pipelineTag(name)), value)
        return value
    }

    private fun registerLastSuccessGauge(name: String): AtomicLong {
        val value = AtomicLong(0)
        meterRegistry.gauge("runlet.pipeline.last.success.epoch.seconds", listOf(pipelineTag(name)), value)
        return value
    }

    private fun registerLastFailureGauge(name: String): AtomicLong {
        val value = AtomicLong(0)
        meterRegistry.gauge("runlet.pipeline.last.failure.epoch.seconds", listOf(pipelineTag(name)), value)
        return value
    }

    private fun pipelineTag(name: String) =
        io.micrometer.core.instrument.Tag
            .of("pipeline", name)
}

private data class PipelineMetricState(
    val running: AtomicInteger,
    val lastSuccessEpochSeconds: AtomicLong,
    val lastFailureEpochSeconds: AtomicLong,
)
