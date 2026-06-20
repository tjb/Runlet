package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source

internal class UncheckpointedRunnablePipeline(
    private val source: Source<*>,
    private val stages: List<PipelineStage>,
    private val sink: Sink<*>,
) : RunnablePipeline {
    override suspend fun run() {
        @Suppress("UNCHECKED_CAST")
        val typedSource = source as Source<Any?>

        @Suppress("UNCHECKED_CAST")
        val typedSink = sink as Sink<Any?>

        typedSource.useReader {
            while (true) {
                val sourceChunk = read() ?: break
                val output = sourceChunk.applyStages(stages) ?: continue
                typedSink.write(output)
                typedSink.commit()
            }
        }
    }
}
