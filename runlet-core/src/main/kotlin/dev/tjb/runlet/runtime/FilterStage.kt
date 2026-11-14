package dev.tjb.runlet.runtime

import dev.tjb.runlet.api.Chunk

internal class FilterStage<T>(
    private val predicate: (T) -> Boolean,
) : PipelineStage {
    override suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?>? =
        chunk.filter { record ->
            predicate(record.castForStage())
        }
}
