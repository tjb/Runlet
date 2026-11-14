package dev.tjb.runlet.runtime

import dev.tjb.runlet.api.Chunk

internal class MapStage<I, O>(
    private val transform: (I) -> O,
) : PipelineStage {
    override suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?> =
        chunk.map { record ->
            transform(record.castForStage())
        }
}
