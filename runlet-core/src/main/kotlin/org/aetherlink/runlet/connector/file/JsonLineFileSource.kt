package org.aetherlink.runlet.connector.file

import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader

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
