package org.aetherlink.runlet.api

interface PipelineObserver {
    fun onPipelineStarted(name: String) = Unit

    fun onPipelineCompleted(name: String) = Unit

    fun onPipelineStopped(name: String) = Unit

    fun onPipelineFailed(
        name: String,
        failure: Throwable,
    ) = Unit

    fun onChunkCommitted(
        name: String,
        records: Int,
    ) = Unit

    object None : PipelineObserver
}
