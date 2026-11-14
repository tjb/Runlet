package dev.tjb.runlet.dsl

import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.RunnablePipeline
import kotlinx.coroutines.CancellationException

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
