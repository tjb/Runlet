package dev.tjb.runlet.runtime

import dev.tjb.runlet.api.Chunk

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
