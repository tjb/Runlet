package org.aetherlink.runlet.api

interface CheckpointableSource<T> {
    suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<T>>.() -> R,
    ): R
}
