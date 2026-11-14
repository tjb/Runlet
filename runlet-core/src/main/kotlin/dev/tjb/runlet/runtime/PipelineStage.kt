package dev.tjb.runlet.runtime

import dev.tjb.runlet.api.Chunk

internal interface PipelineStage {
    suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?>?
}
