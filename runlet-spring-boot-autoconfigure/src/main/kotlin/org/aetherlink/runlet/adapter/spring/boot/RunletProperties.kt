package org.aetherlink.runlet.adapter.spring.boot

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("runlet")
class RunletProperties {
    var enabled: Boolean = true

    var threads: Int = 4
        set(value) {
            require(value > 0) { "runlet.threads must be positive" }
            field = value
        }

    var shutdownTimeout: Duration = Duration.ofSeconds(30)
}
