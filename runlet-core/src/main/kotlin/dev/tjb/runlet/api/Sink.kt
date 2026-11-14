package dev.tjb.runlet.api

interface Sink<T> {
    suspend fun write(chunk: Chunk<T>)

    suspend fun commit() {}
}
