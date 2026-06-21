description = "File source, file checkpoint store, and chunk-file sink for Runlet."

dependencies {
    api(project(":runlet-core"))

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
