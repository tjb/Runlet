package org.aetherlink.runlet.runtime

import org.aetherlink.runlet.api.Chunk

internal typealias PipelineOperation = suspend (Chunk<Any?>) -> Chunk<Any?>?
