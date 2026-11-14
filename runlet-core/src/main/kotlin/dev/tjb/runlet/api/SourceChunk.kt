package dev.tjb.runlet.api

data class SourceChunk<T>(
    val chunk: Chunk<T>,
    val cursorRange: CursorRange,
)
