package org.aetherlink.runlet.adapter.spring

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.aetherlink.runlet.api.RunnablePipeline
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
        if (job?.isActive == true) return

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
        runBlocking {
            withTimeoutOrNull(shutdownTimeout) {
                current.cancelAndJoin()
            }
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

    override fun isRunning(): Boolean = job?.isActive == true

    override fun isAutoStartup(): Boolean = autoStartup

    override fun getPhase(): Int = phase

    fun failure(): Throwable? = failure
}
