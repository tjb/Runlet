package dev.tjb.runlet.testing

import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.Cursor

/**
 * A [CheckpointStore] that keeps the cursor in memory and records every persist.
 *
 * Reuse one instance across runs to simulate restarting a pipeline from its last
 * checkpoint. Use [failOnPersist] to simulate a crash after the sink committed but
 * before the checkpoint advanced.
 */
class InMemoryCheckpointStore(
    initial: Cursor? = null,
) : CheckpointStore {
    private val persistFailures = FailurePoints("persist")
    private val persisted = mutableListOf<Cursor>()

    @Volatile
    private var current: Cursor? = initial

    /** The last successfully persisted cursor, or the initial cursor. */
    val cursor: Cursor? get() = current

    /** Every successfully persisted cursor, in order. */
    val history: List<Cursor> get() = synchronized(persisted) { persisted.toList() }

    /**
     * Makes persist call [call] throw [InjectedFailure] without changing the cursor.
     * Calls are counted from 0 for the life of this store, and each scheduled
     * failure fires once.
     */
    fun failOnPersist(call: Int): InMemoryCheckpointStore = apply { persistFailures.schedule(call) }

    override suspend fun load(): Cursor? = current

    override suspend fun persist(cursor: Cursor) {
        persistFailures.next()
        synchronized(persisted) { persisted += cursor }
        current = cursor
    }
}
