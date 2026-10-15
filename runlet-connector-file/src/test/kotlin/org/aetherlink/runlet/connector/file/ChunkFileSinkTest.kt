package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import kotlin.io.path.createTempDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChunkFileSinkTest {
    @Test
    fun `chunk file sink overwrites replayed cursor range`() =
        runBlocking {
            val directory = createTempDirectory("runlet-chunks")
            val sink = ChunkFileSink.lines(directory)
            val range = CursorRange(Cursor(0), Cursor(4))

            sink.write(Chunk(listOf("old")), range)
            sink.commit()
            sink.write(Chunk(listOf("new")), range)
            sink.commit()

            val output = directory.resolve("chunk-0000000000000-0000000000004.jsonl")
            assertTrue(output.toFile().exists())
            assertEquals("new\n", output.readText())
        }

    @Test
    fun `indexed chunks are numbered sequentially across sink instances`() =
        runBlocking {
            val directory = createTempDirectory("runlet-chunks")
            val first = ChunkFileSink.lines(directory)

            first.write(Chunk(listOf("a")))
            first.commit()
            first.write(Chunk(listOf("b")))
            first.commit()

            val second = ChunkFileSink.lines(directory)
            second.write(Chunk(listOf("c")))
            second.commit()

            assertEquals(
                listOf(
                    "chunk-0000000000000.jsonl",
                    "chunk-0000000000001.jsonl",
                    "chunk-0000000000002.jsonl",
                ),
                directory.listDirectoryEntries().map { it.name }.sorted(),
            )
            assertEquals("c\n", directory.resolve("chunk-0000000000002.jsonl").readText())
        }
}
