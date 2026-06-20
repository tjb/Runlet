package org.aetherlink.runlet.api

interface SourceReader<C> {
    suspend fun read(): C?
}
