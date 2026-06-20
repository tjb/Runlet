package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal suspend fun Chunk<*>.applyOperations(operations: List<PipelineOperation>): Chunk<Any?>? {
    var current: Chunk<Any?>? = asAnyChunk()
    for (operation in operations) {
        current = operation(current ?: return null)
    }
    return current
}

private fun Chunk<*>.asAnyChunk(): Chunk<Any?> = Chunk(records)
