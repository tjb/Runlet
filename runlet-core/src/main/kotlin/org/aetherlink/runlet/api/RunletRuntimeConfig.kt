package org.aetherlink.runlet.api

data class RunletRuntimeConfig(
    val channelCapacity: Int = 4,
) {
    init {
        require(channelCapacity > 0) { "channelCapacity must be positive" }
    }
}
