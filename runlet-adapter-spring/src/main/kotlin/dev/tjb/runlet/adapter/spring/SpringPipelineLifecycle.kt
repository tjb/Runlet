package dev.tjb.runlet.adapter.spring

import dev.tjb.runlet.api.RunnablePipeline
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.commons.logging.LogFactory
import org.springframework.context.SmartLifecycle
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class SpringPipelineLifecycle(
    private val pipeline: RunnablePipeline,
    private val scope: CoroutineScope,
    private val shutdownTimeout: Duration = 30.seconds,
    private val phase: Int = SmartLifecycle.DEFAULT_PHASE,
    private val autoStartup: Boolean = true,
    private val onFailure: (Throwable) -> Unit = {},
) : SmartLifecycle {
    @Volatile
    private var job: Job? = null

    @Volatile
    private var failure: Throwable? = null

    override fun start() {
        val previous = job
        if (previous?.isActive == true) return
        // A run that failed or was cancelled may still be running its cleanup, or be
        // stuck in a blocking call after a shutdown timeout. Starting another run now
        // would let two runs share one source and checkpoint, so wait for it first.
        if (previous != null && !previous.isCompleted) {
            runBlocking {
                withTimeoutOrNull(shutdownTimeout) { previous.join() }
            }
            check(previous.isCompleted) {
                "Previous run of this pipeline is still shutting down"
            }
        }

        failure = null
        job =
            scope
                .launch {
                    pipeline.run()
                }.also { launched ->
                    launched.invokeOnCompletion { throwable ->
                        if (throwable != null && throwable !is CancellationException) {
                            failure = throwable
                            onFailure(throwable)
                        }
                    }
                }
    }

    override fun stop() {
        val current = job ?: return
        val stopped =
            runBlocking {
                withTimeoutOrNull(shutdownTimeout) {
                    current.cancelAndJoin()
                } != null
            }
        if (!stopped) {
            // Keep the job so isRunning stays true and start() refuses a second run
            // until this one finishes.
            logger.warn("Pipeline did not stop within $shutdownTimeout; it is still shutting down")
            return
        }
        if (job === current) {
            job = null
        }
    }

    override fun stop(callback: Runnable) {
        try {
            stop()
        } finally {
            callback.run()
        }
    }

    // A cancelled job is no longer active but may still be running its cleanup, so
    // report it as running until it has completed.
    override fun isRunning(): Boolean = job?.isCompleted == false

    override fun isAutoStartup(): Boolean = autoStartup

    override fun getPhase(): Int = phase

    fun failure(): Throwable? = failure

    private companion object {
        private val logger = LogFactory.getLog(SpringPipelineLifecycle::class.java)
    }
}
