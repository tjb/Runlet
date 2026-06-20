package org.aetherlink.runlet.internal

import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

internal object FileChannelCompat {
    fun forceFile(path: Path) {
        FileChannel.open(path, StandardOpenOption.WRITE).use { channel ->
            channel.force(true)
        }
    }

    fun forceDirectory(path: Path) {
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            channel.force(true)
        }
    }
}
