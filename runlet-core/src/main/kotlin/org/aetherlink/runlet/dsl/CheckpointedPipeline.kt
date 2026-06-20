package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.CheckpointStore
import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.RunnablePipeline
import org.aetherlink.runlet.api.Sink
import org.aetherlink.runlet.runtime.CheckpointedRunnablePipeline
import org.aetherlink.runlet.runtime.EvalMapStage
import org.aetherlink.runlet.runtime.FilterStage
import org.aetherlink.runlet.runtime.MapStage
import org.aetherlink.runlet.runtime.PipelineStage

class CheckpointedPipeline<T> internal constructor(
    internal val source: CheckpointableSource<*>,
    internal val stages: List<PipelineStage>,
    internal val checkpointStore: CheckpointStore,
) {
    fun <R> map(transform: (T) -> R): CheckpointedPipeline<R> = addStage(MapStage(transform))

    fun filter(predicate: (T) -> Boolean): CheckpointedPipeline<T> = addStage(FilterStage(predicate))

    fun <R> evalMap(transform: suspend (T) -> R): CheckpointedPipeline<R> = addStage(EvalMapStage(transform))

    fun sink(sink: Sink<T>): RunnablePipeline = CheckpointedRunnablePipeline(source, stages, checkpointStore, sink)

    private fun <R> addStage(stage: PipelineStage): CheckpointedPipeline<R> = CheckpointedPipeline(source, stages + stage, checkpointStore)
}
