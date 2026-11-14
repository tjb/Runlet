package dev.tjb.runlet.adapter.blocking

import dev.tjb.runlet.api.Cursor
import dev.tjb.runlet.api.SourceChunk

interface BlockingCheckpointableSource<T> {
    fun <R> useReader(
        cursor: Cursor?,
        block: (BlockingSourceReader<SourceChunk<T>>) -> R,
    ): R
}
