package org.aetherlink.runlet.adapter.blocking

import org.aetherlink.runlet.api.Chunk

interface BlockingSource<T> {
    fun <R> useReader(block: (BlockingSourceReader<Chunk<T>>) -> R): R
}
