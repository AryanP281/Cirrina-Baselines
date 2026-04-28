plugins { id("common-conventions")
application
}

dependencies {
    implementation("org.apache.fory:fory-core:0.15.0")
    implementation("org.apache.fory:fory-kotlin:0.15.0")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit-pioneer:junit-pioneer:2.3.0")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

application { mainClass.set("ac.at.uibk.dps.dapr.smartfactory.SmartFactoryKt") }

tasks.test {
    useJUnitPlatform()
}
