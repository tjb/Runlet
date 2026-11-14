package dev.tjb.runlet.dsl

import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.RunnablePipeline
import dev.tjb.runlet.api.Sink
import dev.tjb.runlet.api.Source
import dev.tjb.runlet.runtime.EvalMapStage
import dev.tjb.runlet.runtime.FilterStage
import dev.tjb.runlet.runtime.MapStage
import dev.tjb.runlet.runtime.PipelineStage
import dev.tjb.runlet.runtime.UncheckpointedRunnablePipeline

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
