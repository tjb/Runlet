package dev.tjb.runlet.adapter.blocking

import dev.tjb.runlet.api.Chunk

interface BlockingSink<T> {
    fun write(chunk: Chunk<T>)

    fun commit() {}
}
