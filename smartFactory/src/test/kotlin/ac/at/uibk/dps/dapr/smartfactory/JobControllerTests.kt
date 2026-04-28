package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActorImpl
import io.dapr.Topic
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.http.ResponseEntity
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    properties = ["server.port=8080"]
)
@ActiveProfiles("test")
class JobControllerTests {

    private val daprClient: DaprClient = DaprClientBuilder().build()

    @Autowired
    lateinit var jobDoneStatus: CompletableFuture<Boolean>

    @Test
    fun testJobDoneEventRaising() {
        ActorRuntime.getInstance().registerActor(JobControllerActorImpl::class.java)

        daprClient
            .publishEvent("pubsub", "eProductComplete", mapOf<String, Any>())
            .block()

        assertEquals(true, jobDoneStatus.get(10, TimeUnit.SECONDS))
    }

    @TestConfiguration
    class TestConfig {
        @Bean
        fun jobDoneStatus(): CompletableFuture<Boolean> = CompletableFuture()
    }

    @RestController
    class TestSubscriber(
        private val jobDoneStatus: CompletableFuture<Boolean>
    ) {
        @Topic(name = "eJobDone", pubsubName = "pubsub")
        @PostMapping("/eJobDone")
        fun markJobDone(): ResponseEntity<Void> {
            jobDoneStatus.complete(true)
            return ResponseEntity.ok().build()
        }
    }
}
