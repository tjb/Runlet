package org.aetherlink.runlet.adapter.spring.boot

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.adapter.spring.SpringPipelineLifecycle
import org.aetherlink.runlet.api.PipelineObserver
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.RunnablePipeline
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.health.contributor.Status
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.util.concurrent.Executors
import java.util.function.Supplier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class RunletAutoConfigurationTest {
    private val contextRunner =
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    RunletAutoConfiguration::class.java,
                    RunletHealthAutoConfiguration::class.java,
                    RunletMetricsAutoConfiguration::class.java,
                ),
            )

    @Test
    fun `auto configuration creates dispatcher scope registry and manager`() {
        contextRunner.run { context ->
            assertTrue(context.containsBean("runletDispatcher"))
            assertTrue(context.containsBean("runletScope"))
            assertNotNull(context.getBean(RunletPipelineRegistry::class.java))
            assertNotNull(context.getBean(RunletPipelineManager::class.java))
            assertNotNull(context.getBean("runletHealthIndicator", HealthIndicator::class.java))
            assertEquals(4, context.getBean(RunletRuntimeConfig::class.java).channelCapacity)
        }
    }

    @Test
    fun `runtime config is bound from properties`() {
        contextRunner
            .withPropertyValues("runlet.runtime.channel-capacity=12")
            .run { context ->
                assertEquals(12, context.getBean(RunletRuntimeConfig::class.java).channelCapacity)
            }
    }

    @Test
    fun `user provided runtime config is used`() {
        contextRunner
            .withBean(RunletRuntimeConfig::class.java, Supplier { RunletRuntimeConfig(channelCapacity = 99) })
            .run { context ->
                assertEquals(99, context.getBean(RunletRuntimeConfig::class.java).channelCapacity)
            }
    }

    @Test
    fun `metrics observer is created when meter registry is available`() {
        contextRunner
            .withBean(MeterRegistry::class.java, Supplier { SimpleMeterRegistry() })
            .run { context ->
                assertNotNull(context.getBean("runletMetricsObserver", PipelineObserver::class.java))
            }
    }

    @Test
    fun `metrics observer backs off when disabled`() {
        contextRunner
            .withBean(MeterRegistry::class.java, Supplier { SimpleMeterRegistry() })
            .withPropertyValues("runlet.metrics.enabled=false")
            .run { context ->
                assertFalse(context.containsBean("runletMetricsObserver"))
            }
    }

    @Test
    fun `micrometer observer records pipeline metrics`() {
        val registry = SimpleMeterRegistry()
        val observer =
            MicrometerRunletMetrics(registry, java.time.Clock.fixed(java.time.Instant.ofEpochSecond(42), java.time.ZoneOffset.UTC))

        observer.onPipelineStarted("orders")
        observer.onChunkCommitted("orders", records = 3)
        observer.onPipelineCompleted("orders")
        observer.onPipelineStarted("orders")
        observer.onPipelineStopped("orders")

        assertEquals(
            2.0,
            registry
                .get("runlet.pipeline.starts")
                .tag("pipeline", "orders")
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            registry
                .get("runlet.pipeline.chunks")
                .tag("pipeline", "orders")
                .counter()
                .count(),
        )
        assertEquals(
            3.0,
            registry
                .get("runlet.pipeline.records")
                .tag("pipeline", "orders")
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            registry
                .get("runlet.pipeline.completions")
                .tag("pipeline", "orders")
                .counter()
                .count(),
        )
        assertEquals(
            0.0,
            registry
                .get("runlet.pipeline.running")
                .tag("pipeline", "orders")
                .gauge()
                .value(),
        )
        assertEquals(
            42.0,
            registry
                .get("runlet.pipeline.last.success.epoch.seconds")
                .tag("pipeline", "orders")
                .gauge()
                .value(),
        )
    }

    @Test
    fun `invalid thread count is rejected`() {
        contextRunner
            .withPropertyValues("runlet.threads=0")
            .run { context ->
                assertNotNull(context.startupFailure)
            }
    }

    @Test
    fun `invalid channel capacity is rejected`() {
        contextRunner
            .withPropertyValues("runlet.runtime.channel-capacity=0")
            .run { context ->
                assertNotNull(context.startupFailure)
            }
    }

    @Test
    fun `duplicate pipeline names are rejected`() {
        assertFailsWith<IllegalArgumentException> {
            RunletPipelineRegistry(
                listOf(
                    NamedRunletPipelineLifecycle("orders", testPipelineLifecycle()),
                    NamedRunletPipelineLifecycle("orders", testPipelineLifecycle()),
                ),
            )
        }
    }

    @Test
    fun `auto configuration backs off when disabled`() {
        contextRunner
            .withPropertyValues("runlet.enabled=false")
            .run { context ->
                assertFalse(context.containsBean("runletDispatcher"))
                assertFalse(context.containsBean("runletScope"))
                assertFalse(context.containsBean("runletHealthIndicator"))
            }
    }

    @Test
    fun `health indicator backs off when disabled`() {
        contextRunner
            .withPropertyValues("runlet.health.enabled=false")
            .run { context ->
                assertFalse(context.containsBean("runletHealthIndicator"))
            }
    }

    @Test
    fun `user provided health indicator is used`() {
        val customHealthIndicator = HealthIndicator { Health.status("CUSTOM").build() }

        contextRunner
            .withBean("runletHealthIndicator", HealthIndicator::class.java, Supplier { customHealthIndicator })
            .run { context ->
                assertEquals(customHealthIndicator, context.getBean("runletHealthIndicator", HealthIndicator::class.java))
            }
    }

    @Test
    fun `user provided dispatcher is used`() {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        contextRunner
            .withBean(
                "runletDispatcher",
                ExecutorCoroutineDispatcher::class.java,
                Supplier { dispatcher },
            ).run { context ->
                assertEquals(
                    context.getBean("runletDispatcher"),
                    context.getBean(ExecutorCoroutineDispatcher::class.java),
                )
            }
    }

    @Test
    fun `pipeline registrations are managed by lifecycle manager`() {
        val pipeline = BlockingPipeline()
        val runner =
            contextRunner.withBean(
                RunletPipelineRegistration::class.java,
                Supplier {
                    RunletPipelineRegistration("orders") { pipeline }
                },
            )

        runner.run { context ->
            val registry = context.getBean(RunletPipelineRegistry::class.java)
            val manager = context.getBean(RunletPipelineManager::class.java)

            assertEquals(setOf("orders"), registry.names())

            manager.start()
            runBlocking { pipeline.started.await() }
            assertTrue(manager.isRunning)

            manager.stop()
            assertTrue(runBlocking { pipeline.cancelled.await() })
        }
    }

    @Test
    fun `registry reports registered pipeline failures`() {
        val failure = IllegalStateException("pipeline failed")
        val runner =
            contextRunner.withBean(
                RunletPipelineRegistration::class.java,
                Supplier {
                    RunletPipelineRegistration("orders") { FailingPipeline(failure) }
                },
            )

        runner.run { context ->
            val registry = context.getBean(RunletPipelineRegistry::class.java)
            val manager = context.getBean(RunletPipelineManager::class.java)

            manager.start()

            awaitUntil { registry.failures().isNotEmpty() }
            assertEquals(failure, registry.failures()["orders"])
        }
    }

    @Test
    fun `health indicator reports up when registered pipelines have not failed`() {
        val runner =
            contextRunner.withBean(
                RunletPipelineRegistration::class.java,
                Supplier {
                    RunletPipelineRegistration("orders") { BlockingPipeline() }
                },
            )

        runner.run { context ->
            val health = assertNotNull(context.getBean("runletHealthIndicator", HealthIndicator::class.java).health())

            assertEquals(Status.UP, health.status)
            assertEquals(setOf("orders"), health.details["pipelines"])
        }
    }

    @Test
    fun `health indicator reports down when a pipeline fails`() {
        val failure = IllegalStateException("pipeline failed")
        val runner =
            contextRunner.withBean(
                RunletPipelineRegistration::class.java,
                Supplier {
                    RunletPipelineRegistration("orders") { FailingPipeline(failure) }
                },
            )

        runner.run { context ->
            val healthIndicator = context.getBean("runletHealthIndicator", HealthIndicator::class.java)

            awaitUntil { healthIndicator.health()?.status == Status.DOWN }

            val health = assertNotNull(healthIndicator.health())
            assertEquals(Status.DOWN, health.status)
            assertEquals(mapOf("orders" to "pipeline failed"), health.details["failures"])
        }
    }

    private class BlockingPipeline : RunnablePipeline {
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Boolean>()

        override suspend fun run() {
            try {
                started.complete(Unit)
                awaitCancellation()
            } finally {
                cancelled.complete(true)
            }
        }
    }

    private class FailingPipeline(
        private val failure: Throwable,
    ) : RunnablePipeline {
        override suspend fun run(): Unit = throw failure
    }
}

private fun testPipelineLifecycle(): SpringPipelineLifecycle =
    SpringPipelineLifecycle(
        pipeline =
            object : RunnablePipeline {
                override suspend fun run() = Unit
            },
        scope = CoroutineScope(SupervisorJob()),
    )

private fun awaitUntil(predicate: () -> Boolean) {
    val deadline = System.nanoTime() + 1_000_000_000
    while (System.nanoTime() < deadline) {
        if (predicate()) return
        Thread.sleep(10)
    }
    error("Condition was not met before timeout")
}
