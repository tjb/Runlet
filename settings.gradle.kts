plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "Runlet"
include("runlet-core")
include("runlet-connector-file")
include("runlet-adapter-spring")
include("runlet-spring-boot-autoconfigure")
include("runlet-spring-boot-starter")
