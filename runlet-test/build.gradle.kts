description = "Scripted sources, recording sinks, and failure injection for testing Runlet pipelines."

dependencies {
    api(project(":runlet-core"))

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
