package org.aetherlink.runlet.api

object Sources {
    fun <T> chunks(read: suspend () -> Chunk<T>?): Source<T> =
        object : Source<T> {
            override suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R =
                block(
                    object : SourceReader<Chunk<T>> {
                        override suspend fun read(): Chunk<T>? = read()
                    },
                )
        }

    fun <T> records(
        chunkSize: Int,
        read: suspend (limit: Int) -> List<T>,
    ): Source<T> {
        require(chunkSize > 0) { "chunkSize must be positive" }
        return chunks {
            Chunk.of(read(chunkSize))
        }
    }
}
