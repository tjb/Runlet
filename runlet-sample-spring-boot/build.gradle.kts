description = "Sample Spring Boot application using Runlet."

plugins {
    application
}

dependencies {
    implementation(project(":runlet-spring-boot-starter"))
    implementation(project(":runlet-connector-jackson"))

    implementation("org.springframework.boot:spring-boot-starter:4.0.0")

    testImplementation(kotlin("test"))
    testImplementation("org.springframework.boot:spring-boot-starter-test:4.0.0")
}

application {
    mainClass.set("org.aetherlink.runlet.sample.spring.boot.RunletSampleApplicationKt")
}
