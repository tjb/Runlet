package org.aetherlink.runlet.api

@JvmInline
value class Cursor(
    val value: Long,
) {
    init {
        require(value >= 0) { "Cursor must be non-negative" }
    }
}
