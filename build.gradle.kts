plugins {
    kotlin("jvm") version "2.3.21"
    `java-library`
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"
}

group = "org.aetherlink"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    compileOnly("org.springframework:spring-context:7.0.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation("org.springframework:spring-context:7.0.0")
}

kotlin {
    jvmToolchain(25)
}

tasks.test {
    useJUnitPlatform()
}
