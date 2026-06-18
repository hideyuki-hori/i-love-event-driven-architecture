plugins {
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.mysql.connector)
    implementation(libs.slf4j.simple)
    implementation(libs.kafka.avro.serializer)
    implementation(libs.avro)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

avro {
    setStringType("String")
}

application {
    mainClass = "com.example.LikeWriter"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.register<JavaExec>("runWriter") {
    group = "application"
    mainClass = "com.example.LikeWriter"
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("runReader") {
    group = "application"
    mainClass = "com.example.LikeCdcReader"
    classpath = sourceSets["main"].runtimeClasspath
}
