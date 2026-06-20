package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal class EvalMapStage<I, O>(
    private val transform: suspend (I) -> O,
) : PipelineStage {
    override suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?> =
        Chunk(
            chunk.records.map { record ->
                transform(record.castForStage())
            },
        )
}
