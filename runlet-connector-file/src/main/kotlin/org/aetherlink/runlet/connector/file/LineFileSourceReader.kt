package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import java.io.BufferedInputStream
import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

internal class LineFileSourceReader(
    path: Path,
    cursor: Cursor,
    private val chunkSize: Int,
) : SourceReader<SourceChunk<String>>,
    Closeable {
    private val input = BufferedInputStream(Files.newInputStream(path))
    private var position = 0L
    private var closed = false

    init {
        skipFully(cursor.value)
        position = cursor.value
    }

    override suspend fun read(): SourceChunk<String>? =
        withContext(Dispatchers.IO) {
            check(!closed) { "Reader is closed" }

            val start = position
            val lines = mutableListOf<String>()
            while (lines.size < chunkSize) {
                val line = readLineUtf8() ?: break
                lines += line
            }

            Chunk.of(lines)?.let { chunk ->
                SourceChunk(
                    chunk = chunk,
                    cursorRange =
                        CursorRange(
                            start = Cursor(start),
                            next = Cursor(position),
                        ),
                )
            }
        }

    override fun close() {
        closed = true
        input.close()
    }

    private fun skipFully(bytes: Long) {
        var remaining = bytes
        while (remaining > 0) {
            val skipped = input.skip(remaining)
            if (skipped <= 0) error("Could not resume $bytes bytes into source")
            remaining -= skipped
        }
    }

    private fun readLineUtf8(): String? {
        val bytes = ArrayList<Byte>()
        while (true) {
            val next = input.read()
            if (next == -1) {
                if (bytes.isEmpty()) return null
                return bytes.toByteArray().toString(StandardCharsets.UTF_8)
            }

            position += 1
            if (next == '\n'.code) {
                return bytes.toByteArray().toString(StandardCharsets.UTF_8).removeSuffix("\r")
            }
            bytes += next.toByte()
        }
    }
}
