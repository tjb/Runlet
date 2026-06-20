package org.aetherlink.runlet.api

interface Source<T> {
    suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R
}
