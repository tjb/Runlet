package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.runtime.CheckpointedRunnablePipeline
import org.aetherlink.runlet.runtime.PipelineOperation

class CheckpointablePipeline<T> internal constructor(
    internal val source: CheckpointableSource<*>,
    internal val operations: List<PipelineOperation> = emptyList(),
    internal val checkpointStore: CheckpointStore? = null,
) {
    fun checkpoint(store: CheckpointStore): CheckpointablePipeline<T> = CheckpointablePipeline(source, operations, store)

    fun <R> map(transform: (T) -> R): CheckpointablePipeline<R> = addOperation { chunk -> chunk.map { transform(it as T) } }

    fun filter(predicate: (T) -> Boolean): CheckpointablePipeline<T> = addOperation { chunk -> chunk.filter { predicate(it as T) } }

    fun <R> evalMap(transform: suspend (T) -> R): CheckpointablePipeline<R> =
        addOperation { chunk ->
            Chunk(chunk.records.map { transform(it as T) })
        }

    fun sink(sink: Sink<T>): RunnablePipeline = CheckpointedRunnablePipeline(source, operations, checkpointStore, sink)

    private fun <R> addOperation(operation: PipelineOperation): CheckpointablePipeline<R> =
        CheckpointablePipeline(source, operations + operation, checkpointStore)
}
