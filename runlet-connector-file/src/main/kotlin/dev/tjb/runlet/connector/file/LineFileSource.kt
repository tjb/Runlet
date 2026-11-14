package dev.tjb.runlet.connector.file

import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.Cursor
import dev.tjb.runlet.api.SourceChunk
import dev.tjb.runlet.api.SourceReader
import dev.tjb.runlet.internal.useSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Path

internal class LineFileSource(
    private val path: Path,
    private val chunkSize: Int,
) : CheckpointableSource<String> {
    init {
        require(chunkSize > 0) { "chunkSize must be positive" }
    }

    override suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<String>>.() -> R,
    ): R {
        val reader =
            withContext(Dispatchers.IO) {
                LineFileSourceReader(path, cursor ?: Cursor(0), chunkSize)
            }
        return reader.useSuspend {
            block(reader)
        }
    }
}
