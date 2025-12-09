plugins {
    `java-library`
    `maven-publish`
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

val CALCITE_VERSION = properties.get("calcite.version")
dependencies {
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
    implementation("org.apache.calcite:calcite-core:${CALCITE_VERSION}")
    implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}")
    implementation("org.duckdb:duckdb_jdbc:1.1.3")
    compileOnly("org.projectlombok:lombok:1.18.30") // Use the latest stable version
    annotationProcessor("org.projectlombok:lombok:1.18.30")
    testCompileOnly("org.projectlombok:lombok:1.18.30")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.30")
    testImplementation(platform("org.junit:junit-bom:6.0.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

group = "edu.cmu.cs.db"
version = "1.0-SNAPSHOT"
description = "calcite_app"
java.sourceCompatibility = JavaVersion.VERSION_1_8

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
    options.release.set(17)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "edu.cmu.cs.db.calcite_app.app.App"
    }
}

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}
