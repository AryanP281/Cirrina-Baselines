package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.arm.ArmActor
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
import io.dapr.client.DaprClientBuilder
import org.apache.fory.Fory
import org.apache.fory.ThreadSafeFory
import org.apache.fory.config.Language
import org.apache.fory.memory.MemoryBuffer
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
class ArmTests(
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

    private val threadBuffer = ThreadLocal.withInitial { MemoryBuffer.newHeapBuffer(1024) }

    private val daprClient = DaprClientBuilder().build()

    @Test
    fun testEPickedUpEventRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6013), 0)

            httpServer.createContext("/pickup") { exchange ->
                exchange.use {
                    val response = PickupResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6013"
            val armProxy = ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId("at-0"))

            testSubscriberBehaviour.eArmPickupBehavior = {
                armProxy.initiatePickup()
            }

            val pickedUp = CompletableFuture<Boolean>()
            testSubscriberBehaviour.ePickedUpBehavior = {
                pickedUp.complete(true)
            }

            daprClient.publishEvent("pubsub", "eArmPickup", 0).block()

            assertEquals(true, pickedUp.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEAssemblyCompleteEventRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6014), 0)

            httpServer.createContext("/pickup") { exchange ->
                exchange.use {
                    val response = PickupResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.createContext("/assemble") { exchange ->
                exchange.use {
                    val response = AssembleResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6014"
            val armProxy = ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId("at-1"))

            testSubscriberBehaviour.eArmPickupBehavior = {
                armProxy.initiatePickup()
            }

            val assembled = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eAssemblyCompleteBehavior = {
                assembled.complete(true)
            }

            daprClient.publishEvent("pubsub", "eArmPickup", 0).block()

            assertEquals(true, assembled.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEProductCompleteEventRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6015), 0)

            httpServer.createContext("/pickup") { exchange ->
                exchange.use {
                    val response = PickupResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.createContext("/assemble") { exchange ->
                exchange.use {
                    val response = AssembleResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6015"
            val armProxy = ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId("at-2"))

            testSubscriberBehaviour.eArmPickupBehavior = {
                armProxy.initiatePickup()
            }

            val productComplete = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eProductCompleteBehavior = {
                productComplete.complete(true)
            }

            daprClient.publishEvent("pubsub", "eArmPickup", 0).block()

            assertEquals(true, productComplete.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEProcessMessageEventRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6016), 0)

            httpServer.createContext("/pickup") { exchange ->
                exchange.use {
                    val response = PickupResponse(false)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6016"
            val armProxy = ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId("at-3"))

            testSubscriberBehaviour.eArmPickupBehavior = {
                armProxy.initiatePickup()
            }

            val message = CompletableFuture<String>()
            testSubscriberBehaviour.eProcessMessageBehavior = {
                message.complete(it)
            }

            daprClient.publishEvent("pubsub", "eArmPickup", 0).block()

            assertEquals("Fatal robotic arm failure: Pickup failed...", message.get(5, TimeUnit.SECONDS))
        }
    }

}
