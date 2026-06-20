package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.Chunk
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.runtime.PipelineOperation
import org.aetherlink.runlet.runtime.UncheckpointedRunnablePipeline

class Pipeline<T> internal constructor(
    internal val source: Source<*>,
    internal val operations: List<PipelineOperation> = emptyList(),
) {
    fun <R> map(transform: (T) -> R): Pipeline<R> = addOperation { chunk -> chunk.map { transform(it as T) } }

    fun filter(predicate: (T) -> Boolean): Pipeline<T> = addOperation { chunk -> chunk.filter { predicate(it as T) } }

    fun <R> evalMap(transform: suspend (T) -> R): Pipeline<R> =
        addOperation { chunk ->
            Chunk(chunk.records.map { transform(it as T) })
        }

    fun sink(sink: Sink<T>): RunnablePipeline = UncheckpointedRunnablePipeline(source, operations, sink)

    private fun <R> addOperation(operation: PipelineOperation): Pipeline<R> = Pipeline(source, operations + operation)
}
