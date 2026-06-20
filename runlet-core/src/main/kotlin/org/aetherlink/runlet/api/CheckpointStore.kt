package org.aetherlink.runlet.api

interface CheckpointStore {
    suspend fun load(): Cursor?

    suspend fun persist(cursor: Cursor)
}
