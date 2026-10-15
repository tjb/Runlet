package org.aetherlink.runlet.internal

import java.io.IOException
import java.nio.file.Path

/*
 * File syncs must succeed: a sink commit or checkpoint persist that returns after a
 * failed sync would report durability it does not have.
 */
internal fun fsyncFile(path: Path) {
    FileChannelCompat.forceFile(path)
}

/*
 * Directory syncs make a completed rename durable. Some platforms and file systems
 * cannot open or force directories, so this step is best-effort.
 */
internal fun fsyncDirectory(path: Path) {
    try {
        FileChannelCompat.forceDirectory(path)
    } catch (unsupported: IOException) {
        return
    } catch (unsupported: UnsupportedOperationException) {
        return
    }
}
