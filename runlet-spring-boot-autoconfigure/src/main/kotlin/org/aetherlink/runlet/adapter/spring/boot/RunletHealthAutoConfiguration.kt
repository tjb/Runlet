package org.aetherlink.runlet.adapter.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [RunletAutoConfiguration::class])
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnBean(RunletPipelineRegistry::class)
@ConditionalOnProperty(prefix = "runlet.health", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class RunletHealthAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = ["runletHealthIndicator"])
    fun runletHealthIndicator(registry: RunletPipelineRegistry): HealthIndicator = RunletHealthIndicator(registry)
}

class RunletHealthIndicator(
    private val registry: RunletPipelineRegistry,
) : HealthIndicator {
    override fun health(): Health {
        val failures = registry.failures()
        val builder = if (failures.isEmpty()) Health.up() else Health.down()

        builder.withDetail("pipelines", registry.names())
        if (failures.isNotEmpty()) {
            builder.withDetail(
                "failures",
                failures.mapValues { (_, failure) -> failure.message ?: failure::class.java.name },
            )
        }

        return builder.build()
    }
}
