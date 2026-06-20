package org.aetherlink.runlet.runtime

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds

class UncheckpointedPipelineTest {
    @Test
    fun `pipeline stages preserve mixed type transitions`() =
        runBlocking {
            val sink = CollectingSink<Boolean>()

            Runlet("test") {
                source(ScriptedSource(listOf(1, 2, 3, 4), chunkSize = 2))
                    .map { "n=$it" }
                    .filter { it != "n=2" }
                    .evalMap { it.endsWith("4") }
                    .sink(sink)
            }.run()

            assertEquals(listOf(listOf(false), listOf(false, true)), sink.committedChunks)
        }

    @Test
    fun `bounded channel prevents unbounded upstream reads when sink is blocked`() =
        runBlocking {
            val source = ObservingSource(records = (1..10).toList())
            val sink = BlockingFirstWriteSink<Int>()

            val job =
                launch {
                    Runlet("test", RunletRuntimeConfig(channelCapacity = 1)) {
                        source(source)
                            .sink(sink)
                    }.run()
                }

            source.thirdReadStarted.await()

            assertNull(withTimeoutOrNull(100.milliseconds) { source.fourthReadStarted.await() })

            sink.releaseFirstWrite.complete(Unit)
            job.join()
            assertEquals(11, source.readCount)
        }

    private class ScriptedSource<T>(
        private val records: List<T>,
        private val chunkSize: Int,
    ) : Source<T> {
        override suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R {
            val reader =
                object : SourceReader<Chunk<T>> {
                    private var index = 0

                    override suspend fun read(): Chunk<T>? {
                        if (index >= records.size) return null
                        val start = index
                        val end = minOf(records.size, start + chunkSize)
                        index = end
                        return Chunk(records.subList(start, end))
                    }
                }

            return block(reader)
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

    private class ObservingSource<T>(
        private val records: List<T>,
    ) : Source<T> {
        val thirdReadStarted = CompletableDeferred<Unit>()
        val fourthReadStarted = CompletableDeferred<Unit>()
        var readCount = 0
            private set

        override suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R {
            val reader =
                object : SourceReader<Chunk<T>> {
                    private var index = 0

                    override suspend fun read(): Chunk<T>? {
                        readCount += 1
                        if (readCount == 3) thirdReadStarted.complete(Unit)
                        if (readCount == 4) fourthReadStarted.complete(Unit)
                        if (index >= records.size) return null

                        val record = records[index]
                        index += 1
                        return Chunk(listOf(record))
                    }
                }

            return block(reader)
        }
    }

    private class BlockingFirstWriteSink<T> : Sink<T> {
        val releaseFirstWrite = CompletableDeferred<Unit>()
        private var firstWrite = true

        override suspend fun write(chunk: Chunk<T>) {
            if (firstWrite) {
                firstWrite = false
                releaseFirstWrite.await()
            }
        }
    }
}
