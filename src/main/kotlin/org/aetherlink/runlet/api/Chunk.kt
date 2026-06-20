package org.aetherlink.runlet.api

data class Chunk<T>(
    val records: List<T>,
) : Iterable<T> {
    init {
        require(records.isNotEmpty()) { "Chunk must contain at least one record" }
    }

    val size: Int get() = records.size

    override fun iterator(): Iterator<T> = records.iterator()

    fun <R> map(transform: (T) -> R): Chunk<R> = Chunk(records.map(transform))

    fun filter(predicate: (T) -> Boolean): Chunk<T>? = records.filter(predicate).takeIf { it.isNotEmpty() }?.let(::Chunk)

    companion object {
        fun <T> of(records: Iterable<T>): Chunk<T>? = records.toList().takeIf { it.isNotEmpty() }?.let(::Chunk)
    }
}
