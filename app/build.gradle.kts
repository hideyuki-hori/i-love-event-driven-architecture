plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.mysql.connector)
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
