package org.aetherlink.runlet.testing

/**
 * Thrown by Runlet test doubles at a failure point scheduled with one of their
 * `failOn...` methods.
 */
class InjectedFailure(
    message: String,
) : RuntimeException(message)
