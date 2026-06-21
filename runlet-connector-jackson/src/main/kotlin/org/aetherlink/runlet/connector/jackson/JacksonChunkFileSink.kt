package org.aetherlink.runlet.connector.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import org.aetherlink.runlet.connector.file.ChunkFileSink
import java.nio.file.Path

object JacksonChunkFileSink {
    fun <T : Any> jsonLines(
        directory: String,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): ChunkFileSink<T> = jsonLines(Path.of(directory), objectMapper)

    fun <T : Any> jsonLines(
        directory: Path,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): ChunkFileSink<T> =
        ChunkFileSink.jsonLines(directory) { record ->
            objectMapper.writeValueAsString(record)
        }
}
