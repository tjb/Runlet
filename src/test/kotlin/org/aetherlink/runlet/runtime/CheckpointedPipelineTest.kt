package org.aetherlink.runlet.runtime

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals

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

    private class InMemoryCheckpointStore : CheckpointStore {
        var cursor: Cursor? = null

        override suspend fun load(): Cursor? = cursor

        override suspend fun persist(cursor: Cursor) {
            this.cursor = cursor
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
