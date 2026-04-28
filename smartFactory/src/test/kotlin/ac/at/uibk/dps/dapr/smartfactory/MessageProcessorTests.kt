package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor.MessageProcessorActor
import ac.at.uibk.dps.dapr.smartfactory.config.TestActorsConfig
import ac.at.uibk.dps.dapr.smartfactory.config.TestSubscriberBehaviour
import ac.at.uibk.dps.dapr.smartfactory.services.AssembleResponse
import ac.at.uibk.dps.dapr.smartfactory.services.BeamDetectionResponse
import ac.at.uibk.dps.dapr.smartfactory.services.EmptyRequest
import ac.at.uibk.dps.dapr.smartfactory.services.MessageProcessingRequest
import ac.at.uibk.dps.dapr.smartfactory.services.PhotoScanResponse
import ac.at.uibk.dps.dapr.smartfactory.services.PickupResponse
import ac.at.uibk.dps.dapr.smartfactory.services.Services
import ac.at.uibk.dps.dapr.smartfactory.services.StatisticsRequest
import com.sun.net.httpserver.HttpServer
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import org.apache.fory.Fory
import org.apache.fory.ThreadSafeFory
import org.apache.fory.config.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.use

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    properties = ["server.port=8080"]
)
@Import(TestActorsConfig::class)
class MessageProcessorTests(
    @Autowired val testSubscriberBehaviour: TestSubscriberBehaviour
) {
    private val fory: ThreadSafeFory =
        Fory.builder().withLanguage(Language.XLANG).withRefTracking(true).buildThreadSafeFory().apply {
            register(EmptyRequest::class.java)
            register(BeamDetectionResponse::class.java)
            register(StatisticsRequest::class.java)
            register(MessageProcessingRequest::class.java)
            register(PhotoScanResponse::class.java)
            register(PickupResponse::class.java)
            register(AssembleResponse::class.java)
        }

    private val daprClient: DaprClient = DaprClientBuilder().build()

    @Test
    fun testMessageProcessing()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6003), 0)

            val msgFuture = CompletableFuture<String>()
            httpServer.createContext("/process/email") { exchange ->
                exchange.use {
                    val data = fory.deserialize(exchange.requestBody.readAllBytes()) as MessageProcessingRequest
                    msgFuture.complete(data.msg)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6003"
            val messageProcessorProxy = ActorProxyBuilder(MessageProcessorActor::class.java, ActorClient()).build(ActorId("mpt-0"))

            testSubscriberBehaviour.eProcessMessageBehavior = {
                messageProcessorProxy.processMessage(it)
            }

            daprClient.publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Hello World!")).subscribe()

            assertEquals("Hello World!", msgFuture.get(5, TimeUnit.SECONDS))
        }
    }
}
