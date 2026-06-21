package org.aetherlink.runlet.connector.jackson

import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import kotlin.io.path.createTempDirectory
import kotlin.io.path.readLines
import kotlin.test.Test
import kotlin.test.assertEquals

class JacksonChunkFileSinkTest {
    @Test
    fun `json line sink encodes chunks`() =
        runBlocking {
            val directory = createTempDirectory("runlet-jackson-sink")
            val sink = JacksonChunkFileSink.jsonLines<OrderSummary>(directory)

            sink.write(
                Chunk(
                    listOf(
                        OrderSummary("1", "COMPLETED"),
                        OrderSummary("2", "FAILED"),
                    ),
                ),
                CursorRange(Cursor(0), Cursor(2)),
            )
            sink.commit()

            val output = directory.resolve("chunk-0000000000000-0000000000002.jsonl")
            assertEquals(
                listOf(
                    """{"id":"1","status":"COMPLETED"}""",
                    """{"id":"2","status":"FAILED"}""",
                ),
                output.readLines(),
            )
        }
}

private data class OrderSummary(
    val id: String,
    val status: String,
)
