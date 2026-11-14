package dev.tjb.runlet.connector.file

import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.Cursor
import dev.tjb.runlet.internal.fsyncDirectory
import dev.tjb.runlet.internal.fsyncFile
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class FileCheckpointStore(
    private val path: Path,
) : CheckpointStore {
    constructor(path: String) : this(Path.of(path))

    override suspend fun load(): Cursor? =
        withContext(Dispatchers.IO) {
            if (!Files.exists(path)) return@withContext null
            val text = Files.readString(path, StandardCharsets.UTF_8).trim()
            text.takeIf { it.isNotEmpty() }?.toLongOrNull()?.let(::Cursor)
                ?: error("Invalid checkpoint cursor in $path")
        }

    override suspend fun persist(cursor: Cursor) {
        withContext(Dispatchers.IO) {
            path.parent?.let(Files::createDirectories)
            val temp = path.resolveSibling("${path.fileName}.tmp")
            Files.writeString(
                temp,
                cursor.value.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
            )
            fsyncFile(temp)
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            path.parent?.let(::fsyncDirectory)
        }
    }
}
