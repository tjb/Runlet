package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal suspend fun Chunk<*>.applyStages(stages: List<PipelineStage>): Chunk<Any?>? {
    var current: Chunk<Any?>? = asAnyChunk()
    for (stage in stages) {
        current = stage.apply(current ?: return null)
    }
    return current
}

internal fun Chunk<*>.asAnyChunk(): Chunk<Any?> = Chunk(records)
