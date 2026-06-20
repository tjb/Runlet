package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.CursorRangeSink
import org.aetherlink.runlet.internal.fsync
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class ChunkFileSink<T> private constructor(
    private val directory: Path,
    private val encode: (T) -> String,
) : CursorRangeSink<T> {
    private var pending: PendingWrite? = null

    override suspend fun write(chunk: Chunk<T>) {
        writePending(chunk, cursorRange = null)
    }

    override suspend fun write(
        chunk: Chunk<T>,
        cursorRange: CursorRange,
    ) {
        writePending(chunk, cursorRange)
    }

    private suspend fun writePending(
        chunk: Chunk<T>,
        cursorRange: CursorRange?,
    ) {
        withContext(Dispatchers.IO) {
            Files.createDirectories(directory)
            val fileName =
                if (cursorRange == null) {
                    "chunk-${nextChunkIndex().toString().padStart(13, '0')}.jsonl"
                } else {
                    "chunk-${cursorRange.start.value.toString().padStart(
                        13,
                        '0',
                    )}-${cursorRange.next.value.toString().padStart(13, '0')}.jsonl"
                }
            val finalPath = directory.resolve(fileName)
            val tempPath = directory.resolve("${finalPath.fileName}.tmp")
            val content =
                buildString {
                    for (record in chunk.records) {
                        append(encode(record))
                        append('\n')
                    }
                }

            Files.writeString(
                tempPath,
                content,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
            )
            pending = PendingWrite(tempPath, finalPath)
        }
    }

    override suspend fun commit() {
        val write = pending ?: return
        withContext(Dispatchers.IO) {
            fsync(write.tempPath)
            Files.move(
                write.tempPath,
                write.finalPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
            fsync(directory)
            pending = null
        }
    }

    private fun nextChunkIndex(): Long {
        if (pending != null) error("Previous chunk must be committed before writing another")
        if (!Files.exists(directory)) return 0L
        return Files.list(directory).use { stream ->
            stream
                .map { it.fileName.toString() }
                .filter { it.matches(Regex("""chunk-\d{13}\.jsonl""")) }
                .mapToLong { name -> name.removePrefix("chunk-").removeSuffix(".jsonl").toLongOrNull() ?: -1L }
                .max()
                .orElse(-1L) + 1L
        }
    }

    private data class PendingWrite(
        val tempPath: Path,
        val finalPath: Path,
    )

    companion object {
        fun lines(directory: String): ChunkFileSink<String> = lines(Path.of(directory))

        fun lines(directory: Path): ChunkFileSink<String> = ChunkFileSink(directory) { it }

        fun <T> jsonLines(
            directory: String,
            encode: (T) -> String,
        ): ChunkFileSink<T> = jsonLines(Path.of(directory), encode)

        fun <T> jsonLines(
            directory: Path,
            encode: (T) -> String,
        ): ChunkFileSink<T> = ChunkFileSink(directory, encode)
    }
}
