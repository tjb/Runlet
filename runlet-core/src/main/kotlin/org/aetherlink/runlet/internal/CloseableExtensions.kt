package org.aetherlink.runlet.internal

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable

internal suspend fun <T : Closeable, R> T.useSuspend(block: suspend (T) -> R): R =
    try {
        block(this)
    } finally {
        withContext(Dispatchers.IO) {
            close()
        }
    }
