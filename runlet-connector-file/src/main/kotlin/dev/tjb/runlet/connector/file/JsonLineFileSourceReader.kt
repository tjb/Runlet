package dev.tjb.runlet.connector.file

import dev.tjb.runlet.api.SourceChunk
import dev.tjb.runlet.api.SourceReader

internal class JsonLineFileSourceReader<T>(
    private val delegate: SourceReader<SourceChunk<String>>,
    private val decode: (String) -> T,
) : SourceReader<SourceChunk<T>> {
    override suspend fun read(): SourceChunk<T>? =
        delegate.read()?.let { sourceChunk ->
            SourceChunk(
                chunk = sourceChunk.chunk.map(decode),
                cursorRange = sourceChunk.cursorRange,
            )
        }
}
