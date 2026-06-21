description = "Spring Boot autoconfiguration for Runlet pipelines."

dependencies {
    api(project(":runlet-core"))
    api(project(":runlet-adapter-spring"))
    api("org.springframework.boot:spring-boot-autoconfigure:4.0.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation("org.springframework.boot:spring-boot-starter-test:4.0.0")
}
