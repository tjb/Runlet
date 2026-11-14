package dev.tjb.runlet.runtime

import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.CursorRangeSink
import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.RunnablePipeline
import dev.tjb.runlet.api.Sink

internal class CheckpointedRunnablePipeline(
    private val name: String,
    private val source: CheckpointableSource<*>,
    private val stages: List<PipelineStage>,
    private val checkpointStore: CheckpointStore,
    private val sink: Sink<*>,
    private val config: RunletRuntimeConfig,
) : RunnablePipeline {
    override suspend fun run() {
        @Suppress("UNCHECKED_CAST")
        val typedSource = source as CheckpointableSource<Any?>

        @Suppress("UNCHECKED_CAST")
        val typedSink = sink as Sink<Any?>

        typedSource.useReader(checkpointStore.load()) {
            while (true) {
                val sourceChunk = read() ?: break
                val output = sourceChunk.chunk.applyStages(stages)
                if (output != null) {
                    if (typedSink is CursorRangeSink) {
                        typedSink.write(output, sourceChunk.cursorRange)
                    } else {
                        typedSink.write(output)
                    }
                    typedSink.commit()
                }
                checkpointStore.persist(sourceChunk.cursorRange.next)
                // Notify only after the checkpoint is durable, so a failing observer
                // cannot leave an already-committed chunk to be replayed.
                if (output != null) {
                    config.observer.onChunkCommitted(name, output.records.size)
                }
            }
        }
    }
}
