package org.aetherlink.runlet.api

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class CheckpointableSourcesTest {
    @Test
    fun `long cursor source pages records and advances cursor`() =
        runBlocking {
            val records = listOf(Record(1), Record(2), Record(5))
            val source =
                CheckpointableSources.byLongCursor(
                    chunkSize = 2,
                    read = { after, limit ->
                        records.filter { it.id > after }.take(limit)
                    },
                    cursorOf = { it.id },
                )

            source.useReader(cursor = null) {
                val first = read()
                val second = read()
                val end = read()

                assertEquals(listOf(Record(1), Record(2)), first?.chunk?.records)
                assertEquals(CursorRange(Cursor(0), Cursor(2)), first?.cursorRange)

                assertEquals(listOf(Record(5)), second?.chunk?.records)
                assertEquals(CursorRange(Cursor(2), Cursor(5)), second?.cursorRange)

                assertNull(end)
            }
        }

    @Test
    fun `long cursor source resumes from checkpoint cursor`() =
        runBlocking {
            val records = listOf(Record(1), Record(2), Record(5))
            val source =
                CheckpointableSources.byLongCursor(
                    chunkSize = 2,
                    read = { after, limit ->
                        records.filter { it.id > after }.take(limit)
                    },
                    cursorOf = { it.id },
                )

            source.useReader(cursor = Cursor(2)) {
                val chunk = read()
                val end = read()

                assertEquals(listOf(Record(5)), chunk?.chunk?.records)
                assertEquals(CursorRange(Cursor(2), Cursor(5)), chunk?.cursorRange)
                assertNull(end)
            }
        }

    @Test
    fun `long cursor source rejects non advancing cursor`() =
        runBlocking {
            val source =
                CheckpointableSources.byLongCursor(
                    chunkSize = 1,
                    read = { _, _ -> listOf(Record(0)) },
                    cursorOf = { it.id },
                )

            source.useReader(cursor = Cursor(0)) {
                assertFailsWith<IllegalArgumentException> {
                    read()
                }
            }
        }

    @Test
    fun `chunk source advances cursor between reads`() =
        runBlocking {
            val source =
                CheckpointableSources.chunks<String> { cursor ->
                    when (cursor?.value ?: 0) {
                        0L ->
                            SourceChunk(
                                Chunk(listOf("a")),
                                CursorRange(Cursor(0), Cursor(1)),
                            )
                        1L ->
                            SourceChunk(
                                Chunk(listOf("b")),
                                CursorRange(Cursor(1), Cursor(2)),
                            )
                        else -> null
                    }
                }

            source.useReader(cursor = null) {
                assertEquals(listOf("a"), read()?.chunk?.records)
                assertEquals(listOf("b"), read()?.chunk?.records)
                assertNull(read())
            }
        }

    private data class Record(
        val id: Long,
    )
}
