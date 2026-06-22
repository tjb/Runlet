package org.aetherlink.runlet.api

object CheckpointableSources {
    fun <T> chunks(read: suspend (cursor: Cursor?) -> SourceChunk<T>?): CheckpointableSource<T> =
        object : CheckpointableSource<T> {
            override suspend fun <R> useReader(
                cursor: Cursor?,
                block: suspend SourceReader<SourceChunk<T>>.() -> R,
            ): R =
                block(
                    object : SourceReader<SourceChunk<T>> {
                        private var currentCursor = cursor

                        override suspend fun read(): SourceChunk<T>? {
                            val chunk = read(currentCursor) ?: return null
                            currentCursor = chunk.cursorRange.next
                            return chunk
                        }
                    },
                )
        }

    fun <T> byLongCursor(
        initialCursor: Long = 0,
        chunkSize: Int,
        read: suspend (after: Long, limit: Int) -> List<T>,
        cursorOf: (T) -> Long,
    ): CheckpointableSource<T> {
        require(initialCursor >= 0) { "initialCursor must be non-negative" }
        require(chunkSize > 0) { "chunkSize must be positive" }

        return object : CheckpointableSource<T> {
            override suspend fun <R> useReader(
                cursor: Cursor?,
                block: suspend SourceReader<SourceChunk<T>>.() -> R,
            ): R =
                block(
                    LongCursorSourceReader(
                        currentCursor = cursor?.value ?: initialCursor,
                        chunkSize = chunkSize,
                        read = read,
                        cursorOf = cursorOf,
                    ),
                )
        }
    }
}

private class LongCursorSourceReader<T>(
    private var currentCursor: Long,
    private val chunkSize: Int,
    private val read: suspend (after: Long, limit: Int) -> List<T>,
    private val cursorOf: (T) -> Long,
) : SourceReader<SourceChunk<T>> {
    override suspend fun read(): SourceChunk<T>? {
        val start = currentCursor
        val records = read(start, chunkSize)
        val chunk = Chunk.of(records) ?: return null
        val next = cursorOf(chunk.records.last())

        require(next > start) {
            "cursorOf must return a cursor greater than the previous cursor"
        }

        currentCursor = next
        return SourceChunk(
            chunk = chunk,
            cursorRange =
                CursorRange(
                    start = Cursor(start),
                    next = Cursor(next),
                ),
        )
    }
}
