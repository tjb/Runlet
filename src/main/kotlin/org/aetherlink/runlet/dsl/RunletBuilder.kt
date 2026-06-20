package org.aetherlink.runlet.dsl

import org.aetherlink.runlet.api.CheckpointableSource
import org.aetherlink.runlet.api.Source

class RunletBuilder internal constructor(
    val name: String,
) {
    fun <T> source(source: Source<T>): Pipeline<T> = Pipeline(source)

    fun <T> source(source: CheckpointableSource<T>): CheckpointablePipeline<T> = CheckpointablePipeline(source)
}
