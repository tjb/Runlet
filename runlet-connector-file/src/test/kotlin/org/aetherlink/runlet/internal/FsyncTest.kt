package org.aetherlink.runlet.internal

import java.io.IOException
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertFailsWith

class FsyncTest {
    @Test
    fun `file sync failures propagate`() {
        val missing = createTempDirectory("runlet-fsync").resolve("missing.tmp")

        assertFailsWith<IOException> {
            fsyncFile(missing)
        }
    }

    @Test
    fun `directory sync failures are tolerated`() {
        val missing = createTempDirectory("runlet-fsync").resolve("missing")

        fsyncDirectory(missing)
    }
}
