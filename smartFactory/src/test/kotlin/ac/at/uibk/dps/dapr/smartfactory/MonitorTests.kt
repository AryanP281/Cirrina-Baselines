package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActor
import ac.at.uibk.dps.dapr.smartfactory.actors.monitor.MonitorActor
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

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    properties = ["server.port=8080"]
)
@Import(TestActorsConfig::class)
class MonitorTests(
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
    fun testEScannedEventHandling()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6000), 0)

            val scansCountFuture = CompletableFuture<Int>()
            httpServer.createContext("/statistics") { exchange ->
                exchange.use {
                    val data = fory.deserialize(exchange.requestBody.readAllBytes()) as StatisticsRequest
                    scansCountFuture.complete(data.nScans)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6000"
            val monitorActorProxy = ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId("mt-0"))

            testSubscriberBehaviour.eScannedBehavior = {
                monitorActorProxy.markScanned()
            }

            daprClient.publishEvent("pubsub", "eScanned", mapOf<String,Any>()).block()

            assertEquals(1, scansCountFuture.get(10, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEAssemblyCompleteEventHandling()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6001), 0)
            val assembliesCountFuture = CompletableFuture<Int>()
            httpServer.createContext("/statistics") { exchange ->
                exchange.use {
                    val data = fory.deserialize(exchange.requestBody.readAllBytes()) as StatisticsRequest
                    assembliesCountFuture.complete(data.nAssemblies)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6001"
            val monitorActorProxy = ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId("mt-1"))

            testSubscriberBehaviour.eAssemblyCompleteBehavior = {
                monitorActorProxy.markAssembled()
            }

            daprClient.publishEvent("pubsub", "eAssemblyComplete", mapOf<String,Any>()).block()

            assertEquals(1, assembliesCountFuture.get(10, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testESendStatisticsEventRaisingOnJobDoneTransition()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6002), 0)
            val jobDoneFuture = CompletableFuture<Boolean>()
            httpServer.createContext("/statistics") { exchange ->
                exchange.use {
                    val data = fory.deserialize(exchange.requestBody.readAllBytes()) as StatisticsRequest
                    jobDoneFuture.complete(data.jobDone)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6002"
            val jobControllerProxy = ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId("mt-jc-0"))
            val monitorActorProxy = ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId("mt-2"))

            testSubscriberBehaviour.eProductCompleteBehavior = {
                jobControllerProxy.markProductCompleted()
            }

            testSubscriberBehaviour.eJobDoneBehavior = {
                monitorActorProxy.markJobDone()
            }

            jobControllerProxy.initialize()
            daprClient.publishEvent("pubsub", "eProductComplete", mapOf<String,Any>()).block()

            assertEquals(true, jobDoneFuture.get(5, TimeUnit.SECONDS))
        }
    }

}
