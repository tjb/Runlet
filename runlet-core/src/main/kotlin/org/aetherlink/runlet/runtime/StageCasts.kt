package org.aetherlink.runlet.runtime

/*
 * Pipeline stages are type-safe at the DSL boundary, but the runtime stores a
 * linear list of heterogeneous stages. The JVM cannot represent that changing
 * type parameter across one list, so the internal stage boundary is erased and
 * each named stage owns the single cast back to the input type it was built for.
 */
@Suppress("UNCHECKED_CAST")
internal fun <T> Any?.castForStage(): T = this as T
