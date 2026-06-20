package org.aetherlink.runlet.runtime

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source

internal class UncheckpointedRunnablePipeline(
    private val source: Source<*>,
    private val stages: List<PipelineStage>,
    private val sink: Sink<*>,
    private val config: RunletRuntimeConfig,
) : RunnablePipeline {
    override suspend fun run() =
        coroutineScope {
            // Uncheckpointed pipelines can run stages concurrently because no cursor
            // is advanced from sink durability; checkpointed execution stays serial.
            val channels = List(stages.size + 1) { Channel<ChunkEnvelope>(config.channelCapacity) }

            launchSource(channels.first())
            stages.forEachIndexed { index, stage ->
                launchStage(stage, channels[index], channels[index + 1])
            }
            launchSink(channels.last())
            Unit
        }

    private fun kotlinx.coroutines.CoroutineScope.launchSource(output: Channel<ChunkEnvelope>) =
        launch {
            try {
                typedSource().useReader {
                    while (true) {
                        val sourceChunk = read() ?: break
                        output.send(sourceChunk.asAnyChunk())
                    }
                }
            } finally {
                output.close()
            }
        }

    private fun kotlinx.coroutines.CoroutineScope.launchStage(
        stage: PipelineStage,
        input: Channel<ChunkEnvelope>,
        output: Channel<ChunkEnvelope>,
    ) = launch {
        try {
            for (chunk in input) {
                val transformed = stage.apply(chunk) ?: continue
                output.send(transformed)
            }
        } finally {
            output.close()
        }
    }

    private fun kotlinx.coroutines.CoroutineScope.launchSink(input: Channel<ChunkEnvelope>) =
        launch {
            val typedSink = typedSink()

            for (chunk in input) {
                typedSink.write(chunk)
                typedSink.commit()
            }
        }

    private fun typedSource(): Source<Any?> {
        @Suppress("UNCHECKED_CAST")
        return source as Source<Any?>
    }

    private fun typedSink(): Sink<Any?> {
        @Suppress("UNCHECKED_CAST")
        return sink as Sink<Any?>
    }
}

private typealias ChunkEnvelope = org.aetherlink.runlet.api.Chunk<Any?>
