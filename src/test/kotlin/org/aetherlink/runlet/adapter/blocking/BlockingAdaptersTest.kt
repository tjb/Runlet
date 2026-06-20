package org.aetherlink.runlet.adapter.blocking

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BlockingAdaptersTest {
    @Test
    fun `blocking sink receives write before commit`() =
        runBlocking {
            val events = mutableListOf<String>()
            val sink =
                object : BlockingSink<String> {
                    override fun write(chunk: Chunk<String>) {
                        events += "write:${chunk.records.joinToString(",")}"
                    }

                    override fun commit() {
                        events += "commit"
                    }
                }

            Runlet("test") {
                source(BlockingListSource(listOf("a", "b")).asSource())
                    .sink(sink.asSink())
            }.run()

            assertEquals(
                listOf(
                    "write:a",
                    "commit",
                    "write:b",
                    "commit",
                ),
                events,
            )
        }

    @Test
    fun `blocking source feeds coroutine pipeline`() =
        runBlocking {
            val sink = CollectingBlockingSink<String>()

            Runlet("test") {
                source(BlockingListSource(listOf(1, 2)).asSource())
                    .map { "n=$it" }
                    .sink(sink.asSink())
            }.run()

            assertEquals(listOf(listOf("n=1"), listOf("n=2")), sink.committedChunks)
        }

    @Test
    fun `blocking checkpointable source receives checkpoint cursor`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore(Cursor(2))
            val source = RecordingBlockingCheckpointableSource(listOf("a", "b", "c"))
            val sink = CollectingBlockingSink<String>()

            Runlet("test") {
                source(source.asCheckpointableSource())
                    .checkpoint(checkpoint)
                    .sink(sink.asSink())
            }.run()

            assertEquals(Cursor(2), source.receivedCursor)
            assertEquals(listOf(listOf("c")), sink.committedChunks)
            assertEquals(Cursor(3), checkpoint.cursor)
        }

    @Test
    fun `blocking sink exceptions propagate`() =
        runBlocking {
            val sink =
                object : BlockingSink<String> {
                    override fun write(chunk: Chunk<String>) {
                        error("write failed")
                    }
                }

            assertFailsWith<IllegalStateException> {
                Runlet("test") {
                    source(BlockingListSource(listOf("a")).asSource())
                        .sink(sink.asSink())
                }.run()
            }
        }

    private class BlockingListSource<T>(
        private val records: List<T>,
    ) : BlockingSource<T> {
        override fun <R> useReader(block: (BlockingSourceReader<Chunk<T>>) -> R): R {
            var index = 0
            val reader =
                BlockingSourceReader {
                    if (index >= records.size) return@BlockingSourceReader null
                    val record = records[index]
                    index += 1
                    Chunk(listOf(record))
                }

            return block(reader)
        }
    }

    private class RecordingBlockingCheckpointableSource<T>(
        private val records: List<T>,
    ) : BlockingCheckpointableSource<T> {
        var receivedCursor: Cursor? = null

        override fun <R> useReader(
            cursor: Cursor?,
            block: (BlockingSourceReader<SourceChunk<T>>) -> R,
        ): R {
            receivedCursor = cursor
            var index = cursor?.value?.toInt() ?: 0
            val reader =
                BlockingSourceReader {
                    if (index >= records.size) return@BlockingSourceReader null
                    val start = index
                    val record = records[index]
                    index += 1
                    SourceChunk(
                        chunk = Chunk(listOf(record)),
                        cursorRange = CursorRange(Cursor(start.toLong()), Cursor(index.toLong())),
                    )
                }

            return block(reader)
        }
    }

    private class CollectingBlockingSink<T> : BlockingSink<T> {
        val committedChunks = mutableListOf<List<T>>()
        private var pending: List<T>? = null

        override fun write(chunk: Chunk<T>) {
            pending = chunk.records
        }

        override fun commit() {
            pending?.let(committedChunks::add)
            pending = null
        }
    }

    private class InMemoryCheckpointStore(
        var cursor: Cursor?,
    ) : CheckpointStore {
        override suspend fun load(): Cursor? = cursor

        override suspend fun persist(cursor: Cursor) {
            this.cursor = cursor
        }
    }
}
