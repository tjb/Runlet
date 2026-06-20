dependencies {
    api(project(":runlet-core"))

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    compileOnly("org.springframework:spring-context:7.0.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation("org.springframework:spring-context:7.0.0")
}
