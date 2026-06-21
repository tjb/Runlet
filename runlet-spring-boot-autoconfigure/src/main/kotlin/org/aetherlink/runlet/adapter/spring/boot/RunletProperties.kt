package org.aetherlink.runlet.adapter.spring.boot

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("runlet")
class RunletProperties {
    var enabled: Boolean = true

    /**
     * Size of the dedicated Runlet worker pool used by the default dispatcher.
     */
    var threads: Int = 4
        set(value) {
            require(value > 0) { "runlet.threads must be positive" }
            field = value
        }

    var shutdownTimeout: Duration = Duration.ofSeconds(30)

    var runtime: Runtime = Runtime()

    class Runtime {
        /**
         * Buffer size between source/stage/sink coroutines for uncheckpointed pipelines.
         * Checkpointed pipelines are serial in v0 and do not use this channel fan-out.
         */
        var channelCapacity: Int = 4
            set(value) {
                require(value > 0) { "runlet.runtime.channel-capacity must be positive" }
                field = value
            }
    }
}
