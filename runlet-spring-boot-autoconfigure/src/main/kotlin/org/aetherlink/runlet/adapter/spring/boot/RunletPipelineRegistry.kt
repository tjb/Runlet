package org.aetherlink.runlet.adapter.spring.boot

import org.aetherlink.runlet.adapter.spring.SpringPipelineLifecycle

class RunletPipelineRegistry(
    lifecycles: Collection<NamedRunletPipelineLifecycle>,
) {
    init {
        require(lifecycles.map { it.name }.toSet().size == lifecycles.size) {
            "Runlet pipeline names must be unique"
        }
    }

    private val lifecyclesByName = lifecycles.associateBy { it.name }

    fun names(): Set<String> = lifecyclesByName.keys

    fun lifecycle(name: String): SpringPipelineLifecycle? = lifecyclesByName[name]?.lifecycle

    fun lifecycles(): Collection<SpringPipelineLifecycle> = lifecyclesByName.values.map { it.lifecycle }

    fun failures(): Map<String, Throwable> =
        lifecyclesByName
            .mapNotNull { (name, named) ->
                named.lifecycle.failure()?.let { failure -> name to failure }
            }.toMap()
}

class NamedRunletPipelineLifecycle(
    val name: String,
    val lifecycle: SpringPipelineLifecycle,
)
