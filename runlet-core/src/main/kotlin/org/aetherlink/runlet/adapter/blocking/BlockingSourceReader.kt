package org.aetherlink.runlet.adapter.blocking

fun interface BlockingSourceReader<C> {
    fun read(): C?
}
