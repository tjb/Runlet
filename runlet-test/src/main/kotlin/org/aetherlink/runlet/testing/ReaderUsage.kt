package org.aetherlink.runlet.testing

import java.util.concurrent.atomic.AtomicInteger

internal class ReaderUsage {
    private val opened = AtomicInteger()
    private val closed = AtomicInteger()

    val opens: Int get() = opened.get()
    val closes: Int get() = closed.get()
    val isOpen: Boolean get() = opened.get() > closed.get()

    suspend fun <R> use(block: suspend () -> R): R {
        opened.incrementAndGet()
        try {
            return block()
        } finally {
            closed.incrementAndGet()
        }
    }
}
