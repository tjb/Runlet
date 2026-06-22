package org.aetherlink.runlet.runtime

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.PipelineObserver
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class CheckpointedPipelineTest {
    @Test
    fun `checkpointed pipeline resumes from persisted cursor`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore()
            val sink = CollectingSink<String>()

            Runlet("test") {
                source(ScriptedCheckpointableSource(listOf("a", "b", "c", "d"), chunkSize = 2))
                    .checkpoint(checkpoint)
                    .map { it.uppercase() }
                    .sink(sink)
            }.run()

            assertEquals(listOf(listOf("A", "B"), listOf("C", "D")), sink.committedChunks)
            assertEquals(Cursor(4), checkpoint.cursor)

            val secondSink = CollectingSink<String>()
            Runlet("test") {
                source(ScriptedCheckpointableSource(listOf("a", "b", "c", "d", "e", "f"), chunkSize = 2))
                    .checkpoint(checkpoint)
                    .sink(secondSink)
            }.run()

            assertEquals(listOf(listOf("e", "f")), secondSink.committedChunks)
            assertEquals(Cursor(6), checkpoint.cursor)
        }

    @Test
    fun `checkpointed pipeline applies filter and evalMap before sink commit`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore()
            val sink = CollectingSink<String>()

            Runlet("test") {
                source(ScriptedCheckpointableSource(listOf(1, 2, 3, 4), chunkSize = 2))
                    .checkpoint(checkpoint)
                    .filter { it % 2 == 0 }
                    .evalMap { "n=$it" }
                    .sink(sink)
            }.run()

            assertEquals(listOf(listOf("n=2"), listOf("n=4")), sink.committedChunks)
            assertEquals(Cursor(4), checkpoint.cursor)
        }

    @Test
    fun `checkpoint is persisted after sink commit returns`() =
        runBlocking {
            val events = mutableListOf<String>()
            val checkpoint = RecordingCheckpointStore(events)
            val sink = RecordingSink<String>(events)

            Runlet("test") {
                source(ScriptedCheckpointableSource(listOf("a"), chunkSize = 1))
                    .checkpoint(checkpoint)
                    .sink(sink)
            }.run()

            assertEquals(
                listOf(
                    "write:a",
                    "commit",
                    "persist:1",
                ),
                events,
            )
        }

    @Test
    fun `observer sees started committed chunks and completed`() =
        runBlocking {
            val observer = RecordingObserver()

            Runlet("test", RunletRuntimeConfig(observer = observer)) {
                source(ScriptedCheckpointableSource(listOf("a", "b", "c"), chunkSize = 2))
                    .checkpoint(InMemoryCheckpointStore())
                    .filter { it != "b" }
                    .sink(CollectingSink())
            }.run()

            assertEquals(
                listOf(
                    "started:test",
                    "chunk:test:1",
                    "chunk:test:1",
                    "completed:test",
                ),
                observer.events,
            )
        }

    @Test
    fun `checkpoint does not advance when sink write fails`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore()
            val sink = FailingSink<String>(failOnWrite = true)

            assertFailsWith<IllegalStateException> {
                Runlet("test") {
                    source(ScriptedCheckpointableSource(listOf("a"), chunkSize = 1))
                        .checkpoint(checkpoint)
                        .sink(sink)
                }.run()
            }

            assertNull(checkpoint.cursor)
        }

    @Test
    fun `checkpoint does not advance when sink commit fails`() =
        runBlocking {
            val checkpoint = InMemoryCheckpointStore()
            val sink = FailingSink<String>(failOnCommit = true)

            assertFailsWith<IllegalStateException> {
                Runlet("test") {
                    source(ScriptedCheckpointableSource(listOf("a"), chunkSize = 1))
                        .checkpoint(checkpoint)
                        .sink(sink)
                }.run()
            }

            assertNull(checkpoint.cursor)
        }

    private class InMemoryCheckpointStore : CheckpointStore {
        var cursor: Cursor? = null

        override suspend fun load(): Cursor? = cursor

        override suspend fun persist(cursor: Cursor) {
            this.cursor = cursor
        }
    }

    private class RecordingCheckpointStore(
        private val events: MutableList<String>,
    ) : CheckpointStore {
        override suspend fun load(): Cursor? = null

        override suspend fun persist(cursor: Cursor) {
            events += "persist:${cursor.value}"
        }
    }

    private class CollectingSink<T> : Sink<T> {
        val committedChunks = mutableListOf<List<T>>()
        private var pending: List<T>? = null

        override suspend fun write(chunk: Chunk<T>) {
            pending = chunk.records
        }

        override suspend fun commit() {
            pending?.let(committedChunks::add)
            pending = null
        }
    }

    private class RecordingSink<T>(
        private val events: MutableList<String>,
    ) : Sink<T> {
        override suspend fun write(chunk: Chunk<T>) {
            events += "write:${chunk.records.joinToString(",")}"
        }

        override suspend fun commit() {
            events += "commit"
        }
    }

    private class FailingSink<T>(
        private val failOnWrite: Boolean = false,
        private val failOnCommit: Boolean = false,
    ) : Sink<T> {
        override suspend fun write(chunk: Chunk<T>) {
            if (failOnWrite) error("write failed")
        }

        override suspend fun commit() {
            if (failOnCommit) error("commit failed")
        }
    }

    private class RecordingObserver : PipelineObserver {
        val events = mutableListOf<String>()

        override fun onPipelineStarted(name: String) {
            events += "started:$name"
        }

        override fun onPipelineCompleted(name: String) {
            events += "completed:$name"
        }

        override fun onChunkCommitted(
            name: String,
            records: Int,
        ) {
            events += "chunk:$name:$records"
        }
    }

    private class ScriptedCheckpointableSource<T>(
        private val records: List<T>,
        private val chunkSize: Int,
    ) : CheckpointableSource<T> {
        override suspend fun <R> useReader(
            cursor: Cursor?,
            block: suspend SourceReader<SourceChunk<T>>.() -> R,
        ): R {
            val reader =
                object : SourceReader<SourceChunk<T>> {
                    private var index = cursor?.value?.toInt() ?: 0

                    override suspend fun read(): SourceChunk<T>? {
                        if (index >= records.size) return null
                        val start = index
                        val end = minOf(records.size, start + chunkSize)
                        index = end
                        return SourceChunk(
                            chunk = Chunk(records.subList(start, end)),
                            cursorRange = CursorRange(Cursor(start.toLong()), Cursor(end.toLong())),
                        )
                    }
                }

            return block(reader)
        }
    }
}
