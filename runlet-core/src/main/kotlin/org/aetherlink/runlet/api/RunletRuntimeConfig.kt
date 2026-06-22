package org.aetherlink.runlet.api

data class RunletRuntimeConfig(
    val channelCapacity: Int = 4,
    val observer: PipelineObserver = PipelineObserver.None,
) {
    init {
        require(channelCapacity > 0) { "channelCapacity must be positive" }
    }
}
