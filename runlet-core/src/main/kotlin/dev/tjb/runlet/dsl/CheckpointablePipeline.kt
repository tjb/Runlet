package dev.tjb.runlet.dsl

import dev.tjb.runlet.api.CheckpointStore
import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.runtime.EvalMapStage
import dev.tjb.runlet.runtime.FilterStage
import dev.tjb.runlet.runtime.MapStage
import dev.tjb.runlet.runtime.PipelineStage

class CheckpointablePipeline<T> internal constructor(
    internal val name: String,
    internal val source: CheckpointableSource<*>,
    internal val stages: List<PipelineStage> = emptyList(),
    internal val config: RunletRuntimeConfig,
) {
    fun checkpoint(store: CheckpointStore): CheckpointedPipeline<T> = CheckpointedPipeline(name, source, stages, store, config)

    fun <R> map(transform: (T) -> R): CheckpointablePipeline<R> = addStage(MapStage(transform))

    fun filter(predicate: (T) -> Boolean): CheckpointablePipeline<T> = addStage(FilterStage(predicate))

    fun <R> evalMap(transform: suspend (T) -> R): CheckpointablePipeline<R> = addStage(EvalMapStage(transform))

    private fun <R> addStage(stage: PipelineStage): CheckpointablePipeline<R> = CheckpointablePipeline(name, source, stages + stage, config)
}
