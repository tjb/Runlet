package org.aetherlink.runlet.runtime

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.api.SourceReader
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals

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
}
