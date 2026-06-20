package org.aetherlink.runlet.api

interface RunnablePipeline {
    suspend fun run()

    fun runBlocking() {
        kotlinx.coroutines.runBlocking {
            run()
        }
    }
}
