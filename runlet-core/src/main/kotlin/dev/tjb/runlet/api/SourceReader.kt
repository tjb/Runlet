package dev.tjb.runlet.api

interface SourceReader<C> {
    suspend fun read(): C?
}
