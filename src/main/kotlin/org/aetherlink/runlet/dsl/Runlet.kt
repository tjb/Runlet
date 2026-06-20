package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.RunnablePipeline

class Runlet(
    private val name: String,
    build: RunletBuilder.() -> RunnablePipeline,
) : RunnablePipeline {
    private val pipeline = RunletBuilder(name).build()

    override suspend fun run() {
        pipeline.run()
    }
}
