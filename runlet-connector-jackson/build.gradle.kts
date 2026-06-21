description = "Jackson JSON connectors for Runlet."

dependencies {
    api(project(":runlet-core"))
    api(project(":runlet-connector-file"))
    api("com.fasterxml.jackson.core:jackson-databind:2.22.0")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
