package org.aetherlink.runlet.api

interface Sink<T> {
    suspend fun write(chunk: Chunk<T>)

    suspend fun commit() {}
}
