package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Cursor
import kotlin.io.path.createTempFile
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FileSourceTest {
    @Test
    fun `line source emits chunks with byte cursor ranges`() =
        runBlocking {
            val file = createTempFile("runlet-source", ".txt")
            file.writeText("a\nbb\nccc\n")
            val source = FileSource.lines(file, chunkSize = 2)

            source.useReader(cursor = null) {
                val first = read()
                val second = read()
                val end = read()

                assertEquals(listOf("a", "bb"), first?.chunk?.records)
                assertEquals(Cursor(0), first?.cursorRange?.start)
                assertEquals(Cursor(5), first?.cursorRange?.next)

                assertEquals(listOf("ccc"), second?.chunk?.records)
                assertEquals(Cursor(5), second?.cursorRange?.start)
                assertEquals(Cursor(9), second?.cursorRange?.next)

                assertNull(end)
            }
        }

    @Test
    fun `line source resumes from byte cursor`() =
        runBlocking {
            val file = createTempFile("runlet-source", ".txt")
            file.writeText("a\nbb\nccc\n")
            val source = FileSource.lines(file, chunkSize = 2)

            source.useReader(cursor = Cursor(5)) {
                val chunk = read()
                val end = read()

                assertEquals(listOf("ccc"), chunk?.chunk?.records)
                assertEquals(Cursor(5), chunk?.cursorRange?.start)
                assertEquals(Cursor(9), chunk?.cursorRange?.next)
                assertNull(end)
            }
        }
}
