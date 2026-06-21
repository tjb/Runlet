package org.aetherlink.runlet.connector.jackson

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Cursor
import kotlin.io.path.createTempFile
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class JacksonFileSourceTest {
    @Test
    fun `json line source decodes chunks`() =
        runBlocking {
            val file = createTempFile("runlet-jackson-source", ".jsonl")
            file.writeText(
                """
                {"id":"1","status":"open"}
                {"id":"2","status":"completed"}
                {"id":"3","status":"completed"}
                """.trimIndent() + "\n",
            )
            val source = JacksonFileSource.jsonLines<Order>(file, chunkSize = 2)

            source.useReader(cursor = null) {
                val first = read()
                val second = read()
                val end = read()

                assertEquals(
                    listOf(
                        Order("1", "open"),
                        Order("2", "completed"),
                    ),
                    first?.chunk?.records,
                )
                assertEquals(listOf(Order("3", "completed")), second?.chunk?.records)
                assertNull(end)
            }
        }

    @Test
    fun `json line source supports class based decoding`() =
        runBlocking {
            val file = createTempFile("runlet-jackson-source", ".jsonl")
            file.writeText("""{"id":"1","status":"completed"}""" + "\n")
            val source = JacksonFileSource.jsonLines(file, Order::class.java)

            source.useReader(cursor = null) {
                assertEquals(listOf(Order("1", "completed")), read()?.chunk?.records)
                assertNull(read())
            }
        }

    @Test
    fun `json line source resumes from byte cursor`() =
        runBlocking {
            val file = createTempFile("runlet-jackson-source", ".jsonl")
            val firstLine = """{"id":"1","status":"open"}""" + "\n"
            file.writeText(firstLine + """{"id":"2","status":"completed"}""" + "\n")
            val source = JacksonFileSource.jsonLines<Order>(file, chunkSize = 1)

            source.useReader(cursor = Cursor(firstLine.toByteArray().size.toLong())) {
                val chunk = read()
                val end = read()

                assertEquals(listOf(Order("2", "completed")), chunk?.chunk?.records)
                assertNull(end)
            }
        }
}

private data class Order(
    val id: String,
    val status: String,
)
