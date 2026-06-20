plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "Runlet"
include("runlet-core")
include("runlet-adapter-spring")
