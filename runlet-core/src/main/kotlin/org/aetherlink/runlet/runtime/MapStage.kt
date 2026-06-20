package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal class MapStage<I, O>(
    private val transform: (I) -> O,
) : PipelineStage {
    override suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?> =
        chunk.map { record ->
            transform(record.castForStage())
        }
}
