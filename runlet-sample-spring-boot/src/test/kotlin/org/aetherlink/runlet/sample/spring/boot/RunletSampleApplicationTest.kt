package org.aetherlink.runlet.sample.spring.boot

import org.aetherlink.runlet.adapter.spring.boot.RunletPipelineRegistration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.Test
import kotlin.test.assertEquals

@SpringBootTest
class RunletSampleApplicationTest {
    @Autowired
    private lateinit var registrations: List<RunletPipelineRegistration>

    @Test
    fun registersOrdersPipeline() {
        assertEquals(listOf("orders"), registrations.map { it.name })
    }
}
