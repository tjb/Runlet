plugins {
    kotlin("jvm") version "2.3.21" apply false
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0" apply false
    `maven-publish`
}

allprojects {
    group = "org.aetherlink"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "java-library")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")

    if (!project.name.startsWith("runlet-sample-")) {
        apply(plugin = "maven-publish")
    }

    extensions.configure<JavaPluginExtension>("java") {
        withSourcesJar()
        withJavadocJar()
    }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension>("kotlin") {
        jvmToolchain(25)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    if (plugins.hasPlugin("maven-publish")) {
        extensions.configure<PublishingExtension>("publishing") {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])

                    pom {
                        name.set(project.name)
                        description.set(provider { project.description ?: project.name })
                        url.set("https://github.com/tjb/Runlet")

                        licenses {
                            license {
                                name.set("Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                            }
                        }

                        developers {
                            developer {
                                id.set("tjb")
                                name.set("TJB")
                            }
                        }

                        scm {
                            connection.set("scm:git:https://github.com/tjb/Runlet.git")
                            developerConnection.set("scm:git:ssh://git@github.com:tjb/Runlet.git")
                            url.set("https://github.com/tjb/Runlet")
                        }
                    }
                }
            }
        }
    }
}
