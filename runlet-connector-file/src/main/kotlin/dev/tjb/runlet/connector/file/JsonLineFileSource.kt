package dev.tjb.runlet.connector.file

import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.Cursor
import dev.tjb.runlet.api.SourceChunk
import dev.tjb.runlet.api.SourceReader

internal class JsonLineFileSource<T>(
    private val delegate: CheckpointableSource<String>,
    private val decode: (String) -> T,
) : CheckpointableSource<T> {
    override suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<T>>.() -> R,
    ): R =
        delegate.useReader(cursor) {
            val delegateReader = this
            block(JsonLineFileSourceReader(delegateReader, decode))
        }
}
