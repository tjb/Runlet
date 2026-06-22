package org.aetherlink.runlet.dsl

import kotlinx.coroutines.CancellationException
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.RunnablePipeline

class Runlet(
    private val name: String,
    private val config: RunletRuntimeConfig = RunletRuntimeConfig(),
    build: RunletBuilder.() -> RunnablePipeline,
) : RunnablePipeline {
    private val pipeline = RunletBuilder(name, config).build()

    override suspend fun run() {
        config.observer.onPipelineStarted(name)
        try {
            pipeline.run()
            config.observer.onPipelineCompleted(name)
        } catch (cancellation: CancellationException) {
            config.observer.onPipelineStopped(name)
            throw cancellation
        } catch (failure: Throwable) {
            config.observer.onPipelineFailed(name, failure)
            throw failure
        }
    }
}
