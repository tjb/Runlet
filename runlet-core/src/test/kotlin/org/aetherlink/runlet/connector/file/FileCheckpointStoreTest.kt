package org.aetherlink.runlet.connector.file

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Cursor
import kotlin.io.path.createTempDirectory
import kotlin.io.path.createTempFile
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileCheckpointStoreTest {
    @Test
    fun `load returns null when checkpoint file does not exist`() =
        runBlocking {
            val path = createTempDirectory("runlet-checkpoint").resolve("missing.ckpt")
            val store = FileCheckpointStore(path)

            assertNull(store.load())
        }

    @Test
    fun `load rejects invalid checkpoint contents`() =
        runBlocking {
            val path = createTempFile("runlet-checkpoint", ".ckpt")
            path.writeText("not-a-cursor")
            val store = FileCheckpointStore(path)

            assertFailsWith<IllegalStateException> {
                store.load()
            }
        }

    @Test
    fun `persist creates parent directories and writes cursor durably`() =
        runBlocking {
            val path =
                createTempDirectory("runlet-checkpoint")
                    .resolve("nested")
                    .resolve("pipeline.ckpt")
            val store = FileCheckpointStore(path)

            store.persist(Cursor(42))

            assertTrue(path.toFile().exists())
            assertEquals("42", path.readText())
            assertEquals(Cursor(42), store.load())
        }

    @Test
    fun `persist replaces prior checkpoint cursor`() =
        runBlocking {
            val path = createTempDirectory("runlet-checkpoint").resolve("pipeline.ckpt")
            val store = FileCheckpointStore(path)

            store.persist(Cursor(1))
            store.persist(Cursor(9))

            assertEquals("9", path.readText())
            assertEquals(Cursor(9), store.load())
        }
}
