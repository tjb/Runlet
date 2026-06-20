package org.aetherlink.runlet.connector.file

import org.aetherlink.runlet.api.CheckpointableSource
import java.nio.file.Path

object FileSource {
    fun lines(
        path: String,
        chunkSize: Int = 1024,
    ): CheckpointableSource<String> = lines(Path.of(path), chunkSize)

    fun lines(
        path: Path,
        chunkSize: Int = 1024,
    ): CheckpointableSource<String> = LineFileSource(path, chunkSize)

    fun jsonLines(
        path: String,
        chunkSize: Int = 1024,
    ): CheckpointableSource<String> = lines(path, chunkSize)

    fun jsonLines(
        path: Path,
        chunkSize: Int = 1024,
    ): CheckpointableSource<String> = lines(path, chunkSize)

    fun <T> jsonLines(
        path: String,
        chunkSize: Int = 1024,
        decode: (String) -> T,
    ): CheckpointableSource<T> = JsonLineFileSource(lines(path, chunkSize), decode)

    fun <T> jsonLines(
        path: Path,
        chunkSize: Int = 1024,
        decode: (String) -> T,
    ): CheckpointableSource<T> = JsonLineFileSource(lines(path, chunkSize), decode)
}
