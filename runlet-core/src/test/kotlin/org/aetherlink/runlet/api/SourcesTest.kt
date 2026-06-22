package org.aetherlink.runlet.api

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SourcesTest {
    @Test
    fun `records source emits chunks from read function`() =
        runBlocking {
            val batches =
                ArrayDeque(
                    listOf(
                        listOf("a", "b"),
                        listOf("c"),
                        emptyList(),
                    ),
                )
            val source =
                Sources.records(chunkSize = 2) { limit ->
                    assertEquals(2, limit)
                    batches.removeFirst()
                }

            source.useReader {
                assertEquals(listOf("a", "b"), read()?.records)
                assertEquals(listOf("c"), read()?.records)
                assertNull(read())
            }
        }
}
