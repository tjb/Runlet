package org.aetherlink.runlet.connector.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

fun defaultRunletObjectMapper(): ObjectMapper = jacksonObjectMapper()
