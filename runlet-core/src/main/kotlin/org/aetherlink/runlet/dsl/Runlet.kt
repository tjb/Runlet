package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.RunnablePipeline

class Runlet(
    private val name: String,
    private val config: RunletRuntimeConfig = RunletRuntimeConfig(),
    build: RunletBuilder.() -> RunnablePipeline,
) : RunnablePipeline {
    private val pipeline = RunletBuilder(name, config).build()

    override suspend fun run() {
        pipeline.run()
    }
}
