package dev.tjb.runlet.adapter.blocking

fun interface BlockingSourceReader<C> {
    fun read(): C?
}
