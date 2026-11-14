package org.aetherlink.runlet.testing

/**
 * Counts calls to one operation and throws [InjectedFailure] on scheduled calls.
 *
 * Each scheduled failure fires once, so rerunning a pipeline against the same test
 * double proceeds past the point that failed before.
 */
internal class FailurePoints(
    private val operation: String,
) {
    private val scheduled = mutableSetOf<Int>()
    private var calls = 0

    @Synchronized
    fun schedule(call: Int) {
        require(call >= 0) { "call must be non-negative" }
        scheduled += call
    }

    @Synchronized
    fun next(): Int {
        val call = calls++
        if (scheduled.remove(call)) {
            throw InjectedFailure("Injected $operation failure on call $call")
        }
        return call
    }

    @Synchronized
    fun calls(): Int = calls
}
