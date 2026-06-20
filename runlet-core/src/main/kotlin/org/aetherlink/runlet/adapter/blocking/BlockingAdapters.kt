package org.aetherlink.runlet.adapter.blocking

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.api.SourceChunk
import org.aetherlink.runlet.api.SourceReader

fun <T> BlockingSink<T>.asSink(): Sink<T> = BlockingSinkAdapter(this)

fun <T> BlockingSource<T>.asSource(): Source<T> = BlockingSourceAdapter(this)

fun <T> BlockingCheckpointableSource<T>.asCheckpointableSource(): CheckpointableSource<T> = BlockingCheckpointableSourceAdapter(this)

private class BlockingSinkAdapter<T>(
    private val delegate: BlockingSink<T>,
) : Sink<T> {
    override suspend fun write(chunk: Chunk<T>) {
        withContext(Dispatchers.IO) {
            delegate.write(chunk)
        }
    }

    override suspend fun commit() {
        withContext(Dispatchers.IO) {
            delegate.commit()
        }
    }
}

private class BlockingSourceAdapter<T>(
    private val delegate: BlockingSource<T>,
) : Source<T> {
    override suspend fun <R> useReader(block: suspend SourceReader<Chunk<T>>.() -> R): R =
        withContext(Dispatchers.IO) {
            delegate.useReader { reader ->
                // The blocking source owns reader lifetime synchronously; runBlocking
                // is the bridge that lets Runlet execute its suspend reader block inside it.
                runBlocking {
                    block(BlockingSourceReaderAdapter(reader))
                }
            }
        }
}

private class BlockingCheckpointableSourceAdapter<T>(
    private val delegate: BlockingCheckpointableSource<T>,
) : CheckpointableSource<T> {
    override suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<T>>.() -> R,
    ): R =
        withContext(Dispatchers.IO) {
            delegate.useReader(cursor) { reader ->
                runBlocking {
                    block(BlockingSourceReaderAdapter(reader))
                }
            }
        }
}

private class BlockingSourceReaderAdapter<C>(
    private val delegate: BlockingSourceReader<C>,
) : SourceReader<C> {
    override suspend fun read(): C? =
        withContext(Dispatchers.IO) {
            delegate.read()
        }
}
