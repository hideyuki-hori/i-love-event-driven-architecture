plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.cassandra.driver)
    implementation(libs.slf4j.simple)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

application {
    mainClass = "com.example.LikeCassandra"
    applicationDefaultJvmArgs = listOf(
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow"
    )
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
