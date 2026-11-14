package dev.tjb.runlet.dsl

import dev.tjb.runlet.api.CheckpointableSource
import dev.tjb.runlet.api.RunletRuntimeConfig
import dev.tjb.runlet.api.Source

class RunletBuilder internal constructor(
    val name: String,
    private val config: RunletRuntimeConfig,
) {
    fun <T> source(source: Source<T>): Pipeline<T> = Pipeline(name, source, config = config)

    fun <T> source(source: CheckpointableSource<T>): CheckpointablePipeline<T> = CheckpointablePipeline(name, source, config = config)
}
