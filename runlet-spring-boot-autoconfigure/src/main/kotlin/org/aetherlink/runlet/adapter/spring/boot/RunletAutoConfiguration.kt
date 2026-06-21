package org.aetherlink.runlet.adapter.spring.boot

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import org.aetherlink.runlet.adapter.spring.SpringPipelineLifecycle
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.toKotlinDuration

@AutoConfiguration
@ConditionalOnProperty(prefix = "runlet", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RunletProperties::class)
class RunletAutoConfiguration {
    @Bean(name = ["runletDispatcher"], destroyMethod = "close")
    @ConditionalOnMissingBean(name = ["runletDispatcher"])
    fun runletDispatcher(properties: RunletProperties): ExecutorCoroutineDispatcher =
        Executors
            .newFixedThreadPool(
                properties.threads,
                RunletThreadFactory(),
            ).asCoroutineDispatcher()

    @Bean(name = ["runletScope"])
    @ConditionalOnMissingBean(name = ["runletScope"])
    fun runletScope(runletDispatcher: ExecutorCoroutineDispatcher): CoroutineScope = CoroutineScope(SupervisorJob() + runletDispatcher)

    @Bean
    fun runletPipelineLifecycleFactory(
        runletScope: CoroutineScope,
        properties: RunletProperties,
    ): RunletPipelineLifecycleFactory = RunletPipelineLifecycleFactory(runletScope, properties)

    @Bean
    fun runletPipelineRegistry(
        registrations: List<RunletPipelineRegistration>,
        lifecycleFactory: RunletPipelineLifecycleFactory,
    ): RunletPipelineRegistry =
        RunletPipelineRegistry(
            registrations.map { registration ->
                NamedRunletPipelineLifecycle(
                    name = registration.name,
                    lifecycle = lifecycleFactory.create(registration),
                )
            },
        )

    @Bean
    fun runletPipelineManager(registry: RunletPipelineRegistry): RunletPipelineManager = RunletPipelineManager(registry)
}

class RunletPipelineLifecycleFactory(
    private val scope: CoroutineScope,
    private val properties: RunletProperties,
) {
    fun create(registration: RunletPipelineRegistration): SpringPipelineLifecycle =
        SpringPipelineLifecycle(
            pipeline = registration.pipeline(),
            scope = scope,
            shutdownTimeout = properties.shutdownTimeout.toKotlinDuration(),
        )
}

private class RunletThreadFactory : ThreadFactory {
    private val counter = AtomicInteger(0)

    override fun newThread(runnable: Runnable): Thread = Thread(runnable, "runlet-pipeline-${counter.incrementAndGet()}")
}
