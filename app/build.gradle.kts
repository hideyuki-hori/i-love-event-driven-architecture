plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kafka.clients)
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
    mainClass = "com.example.LikeProducer"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.register<JavaExec>("runProducer") {
    group = "application"
    mainClass = "com.example.LikeProducer"
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("runConsumer") {
    group = "application"
    mainClass = "com.example.LikeConsumer"
    classpath = sourceSets["main"].runtimeClasspath
}
