package org.aetherlink.runlet.adapter.blocking

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

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

    @Test
    fun `cancelling a pipeline exits a blocking source reader scope`() =
        runBlocking {
            val source = EndlessBlockingSource()
            val sink = SuspendingSink<Int>()

            assertStopsWhenCancelled(
                Runlet("test") {
                    source(source.asSource())
                        .sink(sink)
                },
                sink,
            )

            assertTrue(source.released)
        }

    @Test
    fun `cancelling a checkpointed pipeline exits a blocking source reader scope`() =
        runBlocking {
            val source = EndlessBlockingCheckpointableSource()
            val sink = SuspendingSink<Int>()

            assertStopsWhenCancelled(
                Runlet("test") {
                    source(source.asCheckpointableSource())
                        .checkpoint(InMemoryCheckpointStore(null))
                        .sink(sink)
                },
                sink,
            )

            assertTrue(source.released)
        }

    @Test
    fun `sink failures propagate through a blocking checkpointable source`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore(null)
            val sink =
                object : Sink<String> {
                    override suspend fun write(chunk: Chunk<String>) {
                        error("sink failed")
                    }
                }

            val failure =
                assertFailsWith<IllegalStateException> {
                    Runlet("test") {
                        source(RecordingBlockingCheckpointableSource(listOf("a")).asCheckpointableSource())
                            .checkpoint(checkpoint)
                            .sink(sink)
                    }.run()
                }

            assertEquals("sink failed", failure.message)
            assertEquals(null, checkpoint.cursor)
        }

    private suspend fun assertStopsWhenCancelled(
        pipeline: RunnablePipeline,
        sink: SuspendingSink<*>,
    ) {
        // Launched outside the test scope so a pipeline that ignores cancellation
        // fails this assertion instead of hanging the test.
        val job = CoroutineScope(Dispatchers.Default).launch { pipeline.run() }
        withTimeout(5.seconds) { sink.firstWrite.await() }

        val stopped = withTimeoutOrNull(5.seconds) { job.cancelAndJoin() }

        assertNotNull(stopped, "pipeline did not stop after cancellation")
    }

    private class EndlessBlockingSource : BlockingSource<Int> {
        @Volatile
        var released = false

        override fun <R> useReader(block: (BlockingSourceReader<Chunk<Int>>) -> R): R =
            try {
                block { Chunk(listOf(1)) }
            } finally {
                released = true
            }
    }

    private class EndlessBlockingCheckpointableSource : BlockingCheckpointableSource<Int> {
        @Volatile
        var released = false

        override fun <R> useReader(
            cursor: Cursor?,
            block: (BlockingSourceReader<SourceChunk<Int>>) -> R,
        ): R {
            var next = cursor?.value ?: 0
            return try {
                block {
                    val start = next
                    next += 1
                    SourceChunk(Chunk(listOf(1)), CursorRange(Cursor(start), Cursor(next)))
                }
            } finally {
                released = true
            }
        }
    }

    private class SuspendingSink<T> : Sink<T> {
        val firstWrite = CompletableDeferred<Unit>()

        override suspend fun write(chunk: Chunk<T>) {
            firstWrite.complete(Unit)
            awaitCancellation()
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
