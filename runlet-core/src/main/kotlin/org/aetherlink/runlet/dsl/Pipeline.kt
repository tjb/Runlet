package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.api.Source
import org.aetherlink.runlet.runtime.EvalMapStage
import org.aetherlink.runlet.runtime.FilterStage
import org.aetherlink.runlet.runtime.MapStage
import org.aetherlink.runlet.runtime.PipelineStage
import org.aetherlink.runlet.runtime.UncheckpointedRunnablePipeline

class Pipeline<T> internal constructor(
    internal val name: String,
    internal val source: Source<*>,
    internal val stages: List<PipelineStage> = emptyList(),
    internal val config: RunletRuntimeConfig,
) {
    fun <R> map(transform: (T) -> R): Pipeline<R> = addStage(MapStage(transform))

    fun filter(predicate: (T) -> Boolean): Pipeline<T> = addStage(FilterStage(predicate))

    fun <R> evalMap(transform: suspend (T) -> R): Pipeline<R> = addStage(EvalMapStage(transform))

    fun sink(sink: Sink<T>): RunnablePipeline = UncheckpointedRunnablePipeline(name, source, stages, sink, config)

    private fun <R> addStage(stage: PipelineStage): Pipeline<R> = Pipeline(name, source, stages + stage, config)
}
