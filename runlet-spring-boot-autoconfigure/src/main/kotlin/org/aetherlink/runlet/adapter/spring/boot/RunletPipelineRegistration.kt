package org.aetherlink.runlet.adapter.spring.boot

import org.aetherlink.runlet.api.RunnablePipeline

/**
 * Spring Boot applications expose one bean per pipeline using this registration.
 * The factory is invoked once when the lifecycle wrapper is created.
 */
class RunletPipelineRegistration(
    val name: String,
    private val pipelineFactory: () -> RunnablePipeline,
) {
    init {
        require(name.isNotBlank()) { "Pipeline name must not be blank" }
    }

    fun pipeline(): RunnablePipeline = pipelineFactory()
}
