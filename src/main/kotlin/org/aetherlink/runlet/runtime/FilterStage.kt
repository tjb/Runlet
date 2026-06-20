package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal class FilterStage<T>(
    private val predicate: (T) -> Boolean,
) : PipelineStage {
    override suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?>? =
        chunk.filter { record ->
            predicate(record.castForStage())
        }
}
