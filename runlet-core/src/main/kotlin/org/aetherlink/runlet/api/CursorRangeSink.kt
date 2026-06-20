package org.aetherlink.runlet.api

interface CursorRangeSink<T> : Sink<T> {
    suspend fun write(
        chunk: Chunk<T>,
        cursorRange: CursorRange,
    )
}
