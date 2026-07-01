plugins {
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.gradleup.shadow") version "9.4.3"
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
    implementation(libs.kafka.streams)
    implementation(libs.kafka.streams.avro.serde)

    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.clients)
    implementation(libs.flink.connector.base)
    implementation(libs.flink.connector.kafka)
    implementation(libs.flink.avro)
    implementation(libs.flink.avro.confluent.registry)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

tasks.shadowJar {
    mergeServiceFiles()
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

tasks.register<JavaExec>("runStreams") {
    group = "application"
    mainClass = "com.example.LikeCountStreams"
    classpath = sourceSets["main"].runtimeClasspath
}
