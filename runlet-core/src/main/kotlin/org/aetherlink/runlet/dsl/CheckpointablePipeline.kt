package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.runtime.EvalMapStage
import org.aetherlink.runlet.runtime.FilterStage
import org.aetherlink.runlet.runtime.MapStage
import org.aetherlink.runlet.runtime.PipelineStage

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
