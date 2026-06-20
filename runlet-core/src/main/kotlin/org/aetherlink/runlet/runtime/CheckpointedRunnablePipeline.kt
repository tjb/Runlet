package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.CursorRangeSink
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink

internal class CheckpointedRunnablePipeline(
    private val source: CheckpointableSource<*>,
    private val stages: List<PipelineStage>,
    private val checkpointStore: CheckpointStore,
    private val sink: Sink<*>,
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
            }
        }
    }
}
