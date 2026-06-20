package org.aetherlink.runlet.runtime

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class PipelineLifecycleTest {
    @Test
    fun `uncheckpointed pipeline cancellation exits source reader scope`() =
        runBlocking {
            val source = BlockingSource()

            val job =
                launch {
                    Runlet("test") {
                        source(source)
                            .sink(DiscardingSink())
                    }.run()
                }

            source.readStarted.await()
            job.cancelAndJoin()

            assertTrue(source.released)
        }

    @Test
    fun `checkpointed pipeline failure exits source reader scope`() =
        runBlocking {
            val source = OneChunkCheckpointableSource("a")

            assertFailsWith<IllegalStateException> {
                Runlet("test") {
                    source(source)
                        .checkpoint(InMemoryCheckpointStore())
                        .sink(FailingSink())
                }.run()
            }

            assertTrue(source.released)
        }

    private class BlockingSource : Source<String> {
        val readStarted = CompletableDeferred<Unit>()
        var released = false

        override suspend fun <R> useReader(block: suspend SourceReader<Chunk<String>>.() -> R): R {
            val reader =
                object : SourceReader<Chunk<String>> {
                    override suspend fun read(): Chunk<String>? {
                        readStarted.complete(Unit)
                        awaitCancellation()
                    }
                }

            return try {
                block(reader)
            } finally {
                released = true
            }
        }
    }

    private class OneChunkCheckpointableSource<T>(
        private val record: T,
    ) : CheckpointableSource<T> {
        var released = false

        override suspend fun <R> useReader(
            cursor: Cursor?,
            block: suspend SourceReader<SourceChunk<T>>.() -> R,
        ): R {
            val reader =
                object : SourceReader<SourceChunk<T>> {
                    private var read = false

                    override suspend fun read(): SourceChunk<T>? {
                        if (read) return null
                        read = true
                        return SourceChunk(
                            chunk = Chunk(listOf(record)),
                            cursorRange = CursorRange(Cursor(0), Cursor(1)),
                        )
                    }
                }

            return try {
                block(reader)
            } finally {
                released = true
            }
        }
    }

    private class InMemoryCheckpointStore : CheckpointStore {
        override suspend fun load(): Cursor? = null

        override suspend fun persist(cursor: Cursor) = Unit
    }

    private class DiscardingSink<T> : Sink<T> {
        override suspend fun write(chunk: Chunk<T>) = Unit
    }

    private class FailingSink<T> : Sink<T> {
        override suspend fun write(chunk: Chunk<T>) {
            error("write failed")
        }
    }
}
