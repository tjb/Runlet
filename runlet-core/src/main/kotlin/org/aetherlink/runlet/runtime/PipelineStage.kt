package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal interface PipelineStage {
    suspend fun apply(chunk: Chunk<Any?>): Chunk<Any?>?
}
