package dev.tjb.runlet.connector.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.connector.file.FileSource
import java.nio.file.Path

object JacksonFileSource {
    inline fun <reified T : Any> jsonLines(
        path: String,
        chunkSize: Int = 1024,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): CheckpointableSource<T> = jsonLines(Path.of(path), chunkSize, objectMapper)

    inline fun <reified T : Any> jsonLines(
        path: Path,
        chunkSize: Int = 1024,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): CheckpointableSource<T> =
        FileSource.jsonLines(
            path = path,
            chunkSize = chunkSize,
            decode = { line -> objectMapper.readValue<T>(line) },
        )

    fun <T : Any> jsonLines(
        path: String,
        type: Class<T>,
        chunkSize: Int = 1024,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): CheckpointableSource<T> = jsonLines(Path.of(path), type, chunkSize, objectMapper)

    fun <T : Any> jsonLines(
        path: Path,
        type: Class<T>,
        chunkSize: Int = 1024,
        objectMapper: ObjectMapper = defaultRunletObjectMapper(),
    ): CheckpointableSource<T> =
        FileSource.jsonLines(
            path = path,
            chunkSize = chunkSize,
            decode = { line -> objectMapper.readValue(line, type) },
        )
}
