package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

internal class LineFileSourceReader(
    path: Path,
    cursor: Cursor,
    private val chunkSize: Int,
    bufferSize: Int = READ_BUFFER_SIZE,
) : SourceReader<SourceChunk<String>>,
    Closeable {
    init {
        require(bufferSize > 0) { "bufferSize must be positive" }
    }

    private val input = Files.newInputStream(path)
    private val buffer = ByteArray(bufferSize)
    private var bufferPosition = 0
    private var bufferLimit = 0
    private var lineBytes = ByteArray(INITIAL_LINE_CAPACITY)
    private var position = 0L
    private var closed = false

    init {
        try {
            skipFully(cursor.value)
        } catch (failure: Throwable) {
            input.close()
            throw failure
        }
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

    /*
     * Lines are scanned in the read buffer and decoded once, so multi-byte UTF-8
     * characters split across buffer refills stay intact. `position` counts every
     * consumed byte, including the newline, because it becomes the resume cursor.
     */
    private fun readLineUtf8(): String? {
        var lineLength = 0
        while (true) {
            if (bufferPosition == bufferLimit && !fillBuffer()) {
                return if (lineLength == 0) null else String(lineBytes, 0, lineLength, StandardCharsets.UTF_8)
            }

            val newline = indexOfNewline()
            val end = if (newline == -1) bufferLimit else newline
            val segmentLength = end - bufferPosition
            position += segmentLength

            if (newline != -1 && lineLength == 0) {
                val line = decodeLine(buffer, bufferPosition, segmentLength)
                bufferPosition = newline + 1
                position += 1
                return line
            }

            appendToLine(lineLength, segmentLength)
            lineLength += segmentLength
            bufferPosition = end

            if (newline != -1) {
                bufferPosition += 1
                position += 1
                return decodeLine(lineBytes, 0, lineLength)
            }
        }
    }

    private fun fillBuffer(): Boolean {
        val read = input.read(buffer)
        if (read == -1) return false
        bufferPosition = 0
        bufferLimit = read
        return true
    }

    private fun indexOfNewline(): Int {
        for (index in bufferPosition until bufferLimit) {
            if (buffer[index] == NEWLINE) return index
        }
        return -1
    }

    private fun appendToLine(
        lineLength: Int,
        count: Int,
    ) {
        val required = lineLength + count
        if (required > lineBytes.size) {
            lineBytes = lineBytes.copyOf(maxOf(required, lineBytes.size * 2))
        }
        System.arraycopy(buffer, bufferPosition, lineBytes, lineLength, count)
    }

    private fun decodeLine(
        bytes: ByteArray,
        offset: Int,
        length: Int,
    ): String {
        val withoutCarriageReturn =
            if (length > 0 && bytes[offset + length - 1] == CARRIAGE_RETURN) length - 1 else length
        return String(bytes, offset, withoutCarriageReturn, StandardCharsets.UTF_8)
    }

    private companion object {
        const val READ_BUFFER_SIZE = 64 * 1024
        const val INITIAL_LINE_CAPACITY = 256
        const val NEWLINE = '\n'.code.toByte()
        const val CARRIAGE_RETURN = '\r'.code.toByte()
    }
}
