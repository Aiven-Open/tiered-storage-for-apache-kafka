plugins {
    application
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.0.0") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.avro:avro:1.12.0") {
        exclude(group = "org.slf4j")
    }
    implementation("io.confluent:kafka-avro-serializer:7.9.1") {
        exclude(group = "org.slf4j")
    }
    implementation("com.arakelian:faker:4.0.1") {
        exclude(group = "org.slf4j")
    }
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.24.3")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.16.2")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

application {
    mainClass = "io.aiven.Clients"
}
