package org.aetherlink.runlet.testing

import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.CursorRangeSink

/**
 * A sink that records every write and commit, for asserting what a pipeline
 * delivered.
 *
 * Written chunks are pending until [commit] succeeds, then move to
 * [committedChunks]. Use [failOnWrite] and [failOnCommit] to inject failures.
 * In checkpointed pipelines each write also records the source [CursorRange], so
 * tests can assert which ranges were replayed.
 */
class RecordingSink<T> : CursorRangeSink<T> {
    private val writeFailures = FailurePoints("write")
    private val commitFailures = FailurePoints("commit")
    private val allWrites = mutableListOf<SinkWrite<T>>()
    private val pending = mutableListOf<SinkWrite<T>>()
    private val committed = mutableListOf<SinkWrite<T>>()
    private var commitCount = 0

    /** Every successful write, including writes that were never committed. */
    val writes: List<SinkWrite<T>> get() = synchronized(this) { allWrites.toList() }

    /** Writes made durable by a successful [commit], in order. */
    val committedWrites: List<SinkWrite<T>> get() = synchronized(this) { committed.toList() }

    /** The records of each committed chunk, in order. */
    val committedChunks: List<List<T>> get() = committedWrites.map { it.records }

    /** All committed records, flattened in order. Replayed chunks appear more than once. */
    val committedRecords: List<T> get() = committedWrites.flatMap { it.records }

    /** Number of successful commits. */
    val commits: Int get() = synchronized(this) { commitCount }

    /**
     * Makes write call [call] throw [InjectedFailure]. Calls are counted from 0 for
     * the life of this sink, and each scheduled failure fires once.
     */
    fun failOnWrite(call: Int): RecordingSink<T> = apply { writeFailures.schedule(call) }

    /**
     * Makes commit call [call] throw [InjectedFailure] and discard the pending
     * writes, as if the sink rolled them back. Calls are counted from 0 for the life of this sink, and each scheduled
     * failure fires once.
     */
    fun failOnCommit(call: Int): RecordingSink<T> = apply { commitFailures.schedule(call) }

    override suspend fun write(chunk: Chunk<T>) = record(chunk, cursorRange = null)

    override suspend fun write(
        chunk: Chunk<T>,
        cursorRange: CursorRange,
    ) = record(chunk, cursorRange)

    override suspend fun commit() {
        try {
            commitFailures.next()
        } catch (failure: InjectedFailure) {
            synchronized(this) { pending.clear() }
            throw failure
        }
        synchronized(this) {
            committed += pending
            pending.clear()
            commitCount++
        }
    }

    private fun record(
        chunk: Chunk<T>,
        cursorRange: CursorRange?,
    ) {
        writeFailures.next()
        val write = SinkWrite(chunk.records.toList(), cursorRange)
        synchronized(this) {
            allWrites += write
            pending += write
        }
    }
}

/** One chunk written to a [RecordingSink]. [cursorRange] is null for uncheckpointed pipelines. */
data class SinkWrite<T>(
    val records: List<T>,
    val cursorRange: CursorRange?,
)
