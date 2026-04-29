package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.CameraActor
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
class CameraTests(
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
    fun testIsScanningUpdateGeneration()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6006), 0)

            httpServer.createContext("/takephoto") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/scanphoto") { exchange ->
                exchange.use {
                    val response = PhotoScanResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            val scanningStatus = mutableListOf<CompletableFuture<Boolean>>()
            testSubscriberBehaviour.isScanningBehavior = {
                println(it)
                scanningStatus.add(CompletableFuture.completedFuture(it))
            }

            Services.baseUrl = "http://localhost:6006"
            val cameraProxy = ActorProxyBuilder(CameraActor::class.java, ActorClient()).build(ActorId("ct-2"))

            testSubscriberBehaviour.eStartScanBehavior = {
                cameraProxy.startScan()
            }

            daprClient.publishEvent("pubsub", "eStartScan", mapOf<String,Any>()).subscribe()

            Thread.sleep(5000)
            assert(scanningStatus.size >= 2)
            assertEquals(true, scanningStatus[0].get())
            assertEquals(false, scanningStatus[1].get())
        }
    }

    @Test
    fun testValidObjectEventGeneration()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6007), 0)

            httpServer.createContext("/takephoto") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/scanphoto") { exchange ->
                exchange.use {
                    val response = PhotoScanResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6007"
            val cameraProxy = ActorProxyBuilder(CameraActor::class.java, ActorClient()).build(ActorId("ct-3"))

            testSubscriberBehaviour.eStartScanBehavior = {
                cameraProxy.startScan()
            }

            val isObjectValid = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eObjectValidBehavior = {
                isObjectValid.complete(it)
            }

            daprClient.publishEvent("pubsub", "eStartScan", mapOf<String,Any>()).subscribe()

            assertEquals(true, isObjectValid.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testInvalidObjectEventGeneration()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6008), 0)

            httpServer.createContext("/takephoto") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/scanphoto") { exchange ->
                exchange.use {
                    val response = PhotoScanResponse(false)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6008"
            val cameraProxy = ActorProxyBuilder(CameraActor::class.java, ActorClient()).build(ActorId("ct-4"))

            testSubscriberBehaviour.eStartScanBehavior = {
                cameraProxy.startScan()
            }

            val isObjectValid = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eObjectValidBehavior = {
                isObjectValid.complete(it)
            }

            daprClient.publishEvent("pubsub", "eStartScan", mapOf<String,Any>()).subscribe()

            assertEquals(false, isObjectValid.get(5, TimeUnit.SECONDS))
        }
    }

}
