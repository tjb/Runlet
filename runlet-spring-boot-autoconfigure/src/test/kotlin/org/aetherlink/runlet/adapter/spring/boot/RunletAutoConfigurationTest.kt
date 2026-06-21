package org.aetherlink.runlet.adapter.spring.boot

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.adapter.spring.SpringPipelineLifecycle
import org.aetherlink.runlet.api.RunnablePipeline
import org.springframework.boot.autoconfigure.AutoConfigurations
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
                ),
            )

    @Test
    fun `auto configuration creates dispatcher scope registry and manager`() {
        contextRunner.run { context ->
            assertTrue(context.containsBean("runletDispatcher"))
            assertTrue(context.containsBean("runletScope"))
            assertNotNull(context.getBean(RunletPipelineRegistry::class.java))
            assertNotNull(context.getBean(RunletPipelineManager::class.java))
        }
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
