package org.aetherlink.runlet.adapter.blocking

import org.aetherlink.runlet.api.Chunk

interface BlockingSink<T> {
    fun write(chunk: Chunk<T>)

    fun commit() {}
}
