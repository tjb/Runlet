package org.aetherlink.runlet.adapter.blocking

import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.SourceChunk

interface BlockingCheckpointableSource<T> {
    fun <R> useReader(
        cursor: Cursor?,
        block: (BlockingSourceReader<SourceChunk<T>>) -> R,
    ): R
}
