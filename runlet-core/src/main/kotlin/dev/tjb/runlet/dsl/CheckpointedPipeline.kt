package dev.tjb.runlet.dsl

import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.RunnablePipeline
import dev.tjb.runlet.api.Sink
import dev.tjb.runlet.runtime.CheckpointedRunnablePipeline
import dev.tjb.runlet.runtime.EvalMapStage
import dev.tjb.runlet.runtime.FilterStage
import dev.tjb.runlet.runtime.MapStage
import dev.tjb.runlet.runtime.PipelineStage

class CheckpointedPipeline<T> internal constructor(
    internal val name: String,
    internal val source: CheckpointableSource<*>,
    internal val stages: List<PipelineStage>,
    internal val checkpointStore: CheckpointStore,
    internal val config: RunletRuntimeConfig,
) {
    fun <R> map(transform: (T) -> R): CheckpointedPipeline<R> = addStage(MapStage(transform))

    fun filter(predicate: (T) -> Boolean): CheckpointedPipeline<T> = addStage(FilterStage(predicate))

    fun <R> evalMap(transform: suspend (T) -> R): CheckpointedPipeline<R> = addStage(EvalMapStage(transform))

    fun sink(sink: Sink<T>): RunnablePipeline = CheckpointedRunnablePipeline(name, source, stages, checkpointStore, sink, config)

    private fun <R> addStage(stage: PipelineStage): CheckpointedPipeline<R> =
        CheckpointedPipeline(name, source, stages + stage, checkpointStore, config)
}
