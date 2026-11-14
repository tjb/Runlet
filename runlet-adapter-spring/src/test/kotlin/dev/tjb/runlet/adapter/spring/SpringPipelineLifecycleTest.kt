package dev.tjb.runlet.adapter.spring

import dev.tjb.runlet.api.RunnablePipeline
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SpringPipelineLifecycleTest {
    @Test
    fun `start launches pipeline and stop cancels it`() =
        runBlocking {
            val pipeline = BlockingPipeline()
            val lifecycle = SpringPipelineLifecycle(pipeline, CoroutineScope(SupervisorJob()))

            lifecycle.start()
            pipeline.started.await()

            assertTrue(lifecycle.isRunning)

            lifecycle.stop()

            assertTrue(pipeline.cancelled.await())
            assertFalse(lifecycle.isRunning)
        }

    @Test
    fun `stop callback is invoked after stop attempt`() =
        runBlocking {
            val pipeline = BlockingPipeline()
            val lifecycle = SpringPipelineLifecycle(pipeline, CoroutineScope(SupervisorJob()))
            var callbackInvoked = false

            lifecycle.start()
            pipeline.started.await()
            lifecycle.stop { callbackInvoked = true }

            assertTrue(callbackInvoked)
            assertTrue(pipeline.cancelled.await())
        }

    @Test
    fun `run that outlives shutdown timeout stays running and blocks restart`() =
        runBlocking {
            val pipeline = SlowStoppingPipeline()
            val lifecycle =
                SpringPipelineLifecycle(
                    pipeline = pipeline,
                    scope = CoroutineScope(SupervisorJob()),
                    shutdownTimeout = 50.milliseconds,
                )

            lifecycle.start()
            pipeline.started.await()
            lifecycle.stop()

            assertTrue(lifecycle.isRunning)
            assertFailsWith<IllegalStateException> { lifecycle.start() }
            assertEquals(1, pipeline.runs)

            pipeline.release.complete(Unit)
            withTimeout(5.seconds) {
                while (lifecycle.isRunning) delay(10)
            }

            lifecycle.start()
            withTimeout(5.seconds) {
                while (pipeline.runs < 2) delay(10)
            }
            pipeline.release.complete(Unit)
            lifecycle.stop()
            assertFalse(lifecycle.isRunning)
        }

    @Test
    fun `pipeline failure is recorded`() =
        runBlocking {
            val failure = IllegalStateException("pipeline failed")
            val pipeline = FailingPipeline(failure)
            val observedFailure = CompletableDeferred<Throwable>()
            val lifecycle =
                SpringPipelineLifecycle(
                    pipeline = pipeline,
                    scope = CoroutineScope(SupervisorJob()),
                    onFailure = observedFailure::complete,
                )

            lifecycle.start()
            observedFailure.await()

            assertFalse(lifecycle.isRunning)
            assertSame(failure, lifecycle.failure())
        }

    @Test
    fun `auto startup and phase are configurable`() {
        val lifecycle =
            SpringPipelineLifecycle(
                pipeline = CompletedPipeline(),
                scope = CoroutineScope(SupervisorJob()),
                shutdownTimeout = 1.seconds,
                phase = 12,
                autoStartup = false,
            )

        assertFalse(lifecycle.isAutoStartup)
        assertTrue(lifecycle.phase == 12)
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

    private class SlowStoppingPipeline : RunnablePipeline {
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()

        @Volatile
        var runs = 0

        override suspend fun run() {
            runs++
            started.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) { release.await() }
            }
        }
    }

    private class FailingPipeline(
        private val failure: Throwable,
    ) : RunnablePipeline {
        val finished = CompletableDeferred<Unit>()

        override suspend fun run() {
            try {
                throw failure
            } finally {
                delay(1)
                finished.complete(Unit)
            }
        }
    }

    private class CompletedPipeline : RunnablePipeline {
        override suspend fun run() = Unit
    }
}
