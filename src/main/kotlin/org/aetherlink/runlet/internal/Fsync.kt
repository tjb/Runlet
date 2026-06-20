package org.aetherlink.runlet.internal

import java.nio.file.Files
import java.nio.file.Path

internal fun fsync(path: Path) {
    runCatching {
        if (Files.isDirectory(path)) {
            FileChannelCompat.forceDirectory(path)
        } else {
            FileChannelCompat.forceFile(path)
        }
    }
}
