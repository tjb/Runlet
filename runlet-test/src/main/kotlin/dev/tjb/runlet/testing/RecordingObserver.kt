package dev.tjb.runlet.testing

import dev.tjb.runlet.api.PipelineObserver

/** A [PipelineObserver] that records every callback as an [ObserverEvent]. */
class RecordingObserver : PipelineObserver {
    private val recorded = mutableListOf<ObserverEvent>()

    /** Every event observed so far, in order. */
    val events: List<ObserverEvent> get() = synchronized(recorded) { recorded.toList() }

    override fun onPipelineStarted(name: String) = add(ObserverEvent.Started(name))

    override fun onPipelineCompleted(name: String) = add(ObserverEvent.Completed(name))

    override fun onPipelineStopped(name: String) = add(ObserverEvent.Stopped(name))

    override fun onPipelineFailed(
        name: String,
        failure: Throwable,
    ) = add(ObserverEvent.Failed(name, failure))

    override fun onChunkCommitted(
        name: String,
        records: Int,
    ) = add(ObserverEvent.ChunkCommitted(name, records))

    private fun add(event: ObserverEvent) {
        synchronized(recorded) { recorded += event }
    }
}

/** A callback recorded by [RecordingObserver]. */
sealed interface ObserverEvent {
    val pipeline: String

    data class Started(
        override val pipeline: String,
    ) : ObserverEvent

    data class Completed(
        override val pipeline: String,
    ) : ObserverEvent

    data class Stopped(
        override val pipeline: String,
    ) : ObserverEvent

    data class Failed(
        override val pipeline: String,
        val failure: Throwable,
    ) : ObserverEvent

    data class ChunkCommitted(
        override val pipeline: String,
        val records: Int,
    ) : ObserverEvent
}
