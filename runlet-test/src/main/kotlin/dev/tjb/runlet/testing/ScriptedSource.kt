package dev.tjb.runlet.testing

import dev.tjb.runlet.api.Chunk
import dev.tjb.runlet.api.Source
import dev.tjb.runlet.api.SourceReader

/**
 * A [Source] that emits [records] in chunks of [chunkSize] and records how its
 * reader was used.
 *
 * Each call to [useReader] starts again from the first record. Use [failOnRead] to
 * make a read throw [InjectedFailure], and [opens], [closes], and [isOpen] to
 * assert that the pipeline released the reader on completion, failure, or
 * cancellation.
 */
class ScriptedSource<T>(
    records: List<T>,
    private val chunkSize: Int,
) : Source<T> {
    private val records = records.toList()
    private val reads = FailurePoints("read")
    private val usage = ReaderUsage()

    init {
        require(chunkSize > 0) { "chunkSize must be positive" }
    }

    /** Number of times a reader was opened. */
    val opens: Int get() = usage.opens

    /** Number of times a reader scope was exited, normally or not. */
    val closes: Int get() = usage.closes

    /** True while a reader scope is open. */
    val isOpen: Boolean get() = usage.isOpen

    /** Number of read calls across all readers, including failed ones. */
    val readCalls: Int get() = reads.calls()

    /**
     * Makes read call [call] throw [InjectedFailure]. Calls are counted from 0 across
     * every reader this source opens, and each scheduled failure fires once.
     */
    fun failOnRead(call: Int): ScriptedSource<T> = apply { reads.schedule(call) }

    override suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R =
        usage.use {
            var index = 0
            block(
                object : SourceReader<Chunk<T>> {
                    override suspend fun read(): Chunk<T>? {
                        reads.next()
                        if (index >= records.size) return null
                        val end = minOf(records.size, index + chunkSize)
                        val chunk = Chunk(records.subList(index, end).toList())
                        index = end
                        return chunk
                    }
                },
            )
        }
}
