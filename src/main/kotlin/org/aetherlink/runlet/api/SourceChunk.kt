package org.aetherlink.runlet.api

data class SourceChunk<T>(
    val chunk: Chunk<T>,
    val cursorRange: CursorRange,
)
