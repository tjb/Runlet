package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.internal.useSuspend
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
