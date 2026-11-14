package dev.tjb.runlet.adapter.blocking

import dev.tjb.runlet.api.Chunk

interface BlockingSource<T> {
    fun <R> useReader(block: (BlockingSourceReader<Chunk<T>>) -> R): R
}
