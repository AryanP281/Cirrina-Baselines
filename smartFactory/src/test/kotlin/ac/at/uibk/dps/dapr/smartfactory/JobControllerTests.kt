package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActor
import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActorImpl
import ac.at.uibk.dps.dapr.smartfactory.config.TestActorsConfig
import ac.at.uibk.dps.dapr.smartfactory.config.TestSubscriberBehaviour
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import org.springframework.http.ResponseEntity
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    properties = ["server.port=8080"]
)
@Import(TestActorsConfig::class)
class JobControllerTests(
    @Autowired val testSubscriberBehaviour: TestSubscriberBehaviour
) {

    private val daprClient: DaprClient = DaprClientBuilder().build()

    @Test
    fun testJobDoneEventRaising() {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val jobControllerProxy = ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId("jct-0"))
            val jobDoneStatusFuture : CompletableFuture<Boolean> = CompletableFuture()

            testSubscriberBehaviour.eProductCompleteBehavior = {
                jobControllerProxy.markProductCompleted()
            }

            testSubscriberBehaviour.eJobDoneBehavior = {
                jobDoneStatusFuture.complete(true)
            }

            jobControllerProxy.initialize()

            daprClient
                .publishEvent("pubsub", "eProductComplete", mapOf<String, Any>())
                .block()

            assertEquals(true, jobDoneStatusFuture.get(10, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testStartingMessagePublishing()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val jobControllerProxy = ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId("jct-1"))
            val messageFuture : CompletableFuture<String> = CompletableFuture()

            testSubscriberBehaviour.eProcessMessageBehavior = {
                messageFuture.complete(it)
            }

            jobControllerProxy.initialize()

            assertEquals("Job started...", messageFuture.get(10, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testJobDoneMessageProcessingEvent()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val jobControllerProxy = ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId("jct-2"))
            val messageFuture : CompletableFuture<String> = CompletableFuture()
            val eProcessMessageHitCount = AtomicInteger(0)

            testSubscriberBehaviour.eProductCompleteBehavior = {
                jobControllerProxy.markProductCompleted()
            }

            testSubscriberBehaviour.eProcessMessageBehavior = {
                if(eProcessMessageHitCount.incrementAndGet() == 2)
                    messageFuture.complete(it)
            }

            jobControllerProxy.initialize()

            daprClient
                .publishEvent("pubsub", "eProductComplete", mapOf<String, Any>())
                .block()

            assertEquals("Job done...", messageFuture.get(10, TimeUnit.SECONDS))

        }
    }

}
