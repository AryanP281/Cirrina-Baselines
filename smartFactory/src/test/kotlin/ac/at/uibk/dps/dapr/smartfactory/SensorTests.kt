package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.SensorActor
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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.use

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    properties = ["server.port=8080"]
)
@Import(TestActorsConfig::class)
class SensorTests(
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

    private val daprClient: DaprClient = DaprClientBuilder().build()

    @Test
    fun testStartSensorBehavior()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6004), 0)

            httpServer.createContext("/detectbeam/start") { exchange ->
                exchange.use {
                    val response = BeamDetectionResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            val scanStartedFuture = CompletableFuture<Boolean>()
            val counts = AtomicInteger(0)
            testSubscriberBehaviour.eStartScanBehavior = {
                if(counts.incrementAndGet() == 2)
                    scanStartedFuture.complete(true)
            }

            Services.baseUrl = "http://localhost:6004"
            val sensorProxy = ActorProxyBuilder(SensorActor::class.java, ActorClient()).build(ActorId("start-st-0"))
            sensorProxy.initialize()

            assertEquals(true, scanStartedFuture.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEndSensorBehavior()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6005), 0)

            httpServer.createContext("/detectbeam/end") { exchange ->
                exchange.use {
                    val response = BeamDetectionResponse(true)

                    val buffer = threadBuffer.get().apply { writerIndex(0) }
                    fory.serialize(buffer, response)

                    exchange.sendResponseHeaders(200, buffer.writerIndex().toLong())
                    exchange.responseBody.use { stream -> stream.write(buffer.getBytes(0, buffer.writerIndex())) }
                }
            }
            httpServer.start()

            val unloadStartedFuture = CompletableFuture<Boolean>()
            val counts = AtomicInteger(0)
            testSubscriberBehaviour.eStartUnloadBehavior = {
                if(counts.incrementAndGet() == 2)
                    unloadStartedFuture.complete(true)
            }

            Services.baseUrl = "http://localhost:6005"
            val sensorProxy = ActorProxyBuilder(SensorActor::class.java, ActorClient()).build(ActorId("end-st-1"))
            sensorProxy.initialize()

            assertEquals(true, unloadStartedFuture.get(5, TimeUnit.SECONDS))
        }
    }

}
