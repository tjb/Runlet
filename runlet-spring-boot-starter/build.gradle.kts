description = "Spring Boot starter for Runlet pipelines."

dependencies {
    api(project(":runlet-core"))
    api(project(":runlet-adapter-spring"))
    api(project(":runlet-spring-boot-autoconfigure"))

    api("org.springframework.boot:spring-boot-starter:4.0.0")
    api("org.springframework.boot:spring-boot-health:4.0.0")
}
