description = "Spring SmartLifecycle adapter for Runlet pipelines."

dependencies {
    api(project(":runlet-core"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    api("org.springframework:spring-context:7.0.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
