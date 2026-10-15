package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Cursor
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createTempFile
import kotlin.io.path.writeBytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LineFileSourceReaderTest {
    private val content =
        listOf(
            "plain\n",
            "\n",
            "crlf\r\n",
            "two-byte é\n",
            "three-byte 中\n",
            "four-byte 😀\n",
            "x".repeat(300) + "😀" + "\r\n",
            "\r\n",
            "inner\rcarriage\n",
            "no trailing newline\r",
        ).joinToString("").toByteArray(StandardCharsets.UTF_8)

    @Test
    fun `lines and cursors are independent of read buffer boundaries`() =
        runBlocking {
            val file = createTempFile("runlet-lines", ".txt")
            file.writeBytes(content)
            val expected = expectedLines(content)

            for (bufferSize in (1..9) + 64 * 1024) {
                assertEquals(expected, readLines(file, Cursor(0), bufferSize), "bufferSize=$bufferSize")
            }
        }

    @Test
    fun `reader resumes from every line cursor`() =
        runBlocking {
            val file = createTempFile("runlet-lines", ".txt")
            file.writeBytes(content)
            val expected = expectedLines(content)

            for (bufferSize in listOf(1, 3, 64 * 1024)) {
                expected.forEachIndexed { index, line ->
                    assertEquals(
                        expected.drop(index + 1),
                        readLines(file, Cursor(line.next), bufferSize),
                        "bufferSize=$bufferSize resume=${line.next}",
                    )
                }
            }
        }

    @Test
    fun `resuming past the end of the file fails`() {
        val file = createTempFile("runlet-lines", ".txt")
        file.writeBytes(content)

        val failure =
            assertFailsWith<IllegalStateException> {
                LineFileSourceReader(file, Cursor(content.size + 1L), chunkSize = 1)
            }

        assertEquals("Could not resume ${content.size + 1} bytes into source", failure.message)
    }

    @Test
    fun `failed resume closes the file`() {
        val descriptors = Path.of("/proc/self/fd")
        assumeTrue(Files.isDirectory(descriptors), "needs /proc/self/fd to count open files")
        val file = createTempFile("runlet-lines", ".txt")
        file.writeBytes(content)
        val openBefore = Files.list(descriptors).use { it.count() }

        repeat(50) {
            assertFailsWith<IllegalStateException> {
                LineFileSourceReader(file, Cursor(content.size + 1L), chunkSize = 1)
            }
        }

        val openAfter = Files.list(descriptors).use { it.count() }
        assertTrue(openAfter - openBefore < 10, "open files grew from $openBefore to $openAfter")
    }

    private suspend fun readLines(
        file: Path,
        cursor: Cursor,
        bufferSize: Int,
    ): List<ExpectedLine> =
        LineFileSourceReader(file, cursor, chunkSize = 1, bufferSize = bufferSize).use { reader ->
            buildList {
                while (true) {
                    val sourceChunk = reader.read() ?: break
                    add(ExpectedLine(sourceChunk.chunk.records.single(), sourceChunk.cursorRange.next.value))
                }
            }
        }

    // Mirrors the reader contract: split on LF, drop one trailing CR from LF-terminated
    // lines, and keep an unterminated final line as-is.
    private fun expectedLines(bytes: ByteArray): List<ExpectedLine> {
        val lines = mutableListOf<ExpectedLine>()
        var lineStart = 0
        for (index in bytes.indices) {
            if (bytes[index] == '\n'.code.toByte()) {
                val text = String(bytes, lineStart, index - lineStart, StandardCharsets.UTF_8).removeSuffix("\r")
                lines += ExpectedLine(text, index + 1L)
                lineStart = index + 1
            }
        }
        if (lineStart < bytes.size) {
            lines += ExpectedLine(String(bytes, lineStart, bytes.size - lineStart, StandardCharsets.UTF_8), bytes.size.toLong())
        }
        return lines
    }

    private data class ExpectedLine(
        val text: String,
        val next: Long,
    )
}
