package org.aetherlink.runlet.testing

import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader

/**
 * A [CheckpointableSource] that emits [records] in chunks of [chunkSize], using
 * each record's index in [records] as its cursor.
 *
 * A chunk covering records `start until next` carries `CursorRange(Cursor(start),
 * Cursor(next))`, and a reader opened with `Cursor(n)` resumes at record `n`. Use
 * [failOnRead] to inject read failures and [resumeCursors] to assert where each run
 * resumed.
 */
class ScriptedCheckpointableSource<T>(
    records: List<T>,
    private val chunkSize: Int,
) : CheckpointableSource<T> {
    private val records = records.toList()
    private val reads = FailurePoints("read")
    private val usage = ReaderUsage()
    private val resumes = mutableListOf<Cursor?>()

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

    /** The cursor each reader was opened with, in order. `null` means from the start. */
    val resumeCursors: List<Cursor?> get() = synchronized(resumes) { resumes.toList() }

    /**
     * Makes read call [call] throw [InjectedFailure]. Calls are counted from 0 across
     * every reader this source opens, and each scheduled failure fires once.
     */
    fun failOnRead(call: Int): ScriptedCheckpointableSource<T> = apply { reads.schedule(call) }

    override suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<T>>.() -> R,
    ): R {
        val start = cursor?.value ?: 0
        require(start <= records.size) {
            "Cursor $start is past the end of ${records.size} scripted records"
        }
        synchronized(resumes) { resumes += cursor }
        return usage.use {
            var index = start.toInt()
            block(
                object : SourceReader<SourceChunk<T>> {
                    override suspend fun read(): SourceChunk<T>? {
                        reads.next()
                        if (index >= records.size) return null
                        val end = minOf(records.size, index + chunkSize)
                        val chunk =
                            SourceChunk(
                                chunk = Chunk(records.subList(index, end).toList()),
                                cursorRange = CursorRange(Cursor(index.toLong()), Cursor(end.toLong())),
                            )
                        index = end
                        return chunk
                    }
                },
            )
        }
    }
}
