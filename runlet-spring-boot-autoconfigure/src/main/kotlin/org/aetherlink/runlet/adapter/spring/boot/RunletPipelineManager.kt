package org.aetherlink.runlet.adapter.spring.boot

import org.springframework.context.SmartLifecycle

class RunletPipelineManager(
    private val registry: RunletPipelineRegistry,
) : SmartLifecycle {
    @Volatile
    private var running: Boolean = false

    override fun start() {
        registry.lifecycles().forEach { it.start() }
        running = true
    }

    override fun stop() {
        try {
            registry.lifecycles().forEach { it.stop() }
        } finally {
            running = false
        }
    }

    override fun stop(callback: Runnable) {
        try {
            stop()
        } finally {
            callback.run()
        }
    }

    // Reports the manager's own lifecycle state. Tying it to every pipeline still
    // running would make Spring skip stop() once any batch pipeline completes.
    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true
}
