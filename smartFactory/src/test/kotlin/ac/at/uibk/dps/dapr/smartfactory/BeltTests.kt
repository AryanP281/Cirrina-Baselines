package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActor
import ac.at.uibk.dps.dapr.smartfactory.config.TestActorsConfig
import ac.at.uibk.dps.dapr.smartfactory.config.TestSubscriberBehaviour
import ac.at.uibk.dps.dapr.smartfactory.services.PhotoScanResponse
import ac.at.uibk.dps.dapr.smartfactory.services.Services
import com.sun.net.httpserver.HttpServer
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClientBuilder
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
class BeltTests(
    @Autowired val testSubscriberBehaviour: TestSubscriberBehaviour
) {

    private val daprClient = DaprClientBuilder().build()

    @Test
    fun testIsUnloadingStatusChangeOnTransitioningToUnloadingState()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6009), 0)

            httpServer.createContext("/stopbelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/movebelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6009"
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-0"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            testSubscriberBehaviour.eStartUnloadBehavior = {
                beltProxy.startUnloading()
            }

            testSubscriberBehaviour.ePickedUpBehavior = {
                beltProxy.markPickedUp()
            }

            val unloadingStatus = mutableListOf<CompletableFuture<Boolean>>()
            testSubscriberBehaviour.isUnloadingBehavior = {
                unloadingStatus.add(CompletableFuture.completedFuture(it))
            }

            daprClient.publishEvent("pubsub", "eObjectValid", true).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "eStartUnload", 0).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "ePickedUp", 0).block()

            Thread.sleep(3000)
            assert(unloadingStatus.size >= 2)
            assertEquals(true, unloadingStatus[0].get())
            assertEquals(false, unloadingStatus[1].get())
        }
    }

    @Test
    fun testIsUnloadingStatusChangeOnTransitioningToJobDoneState()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val httpServer = HttpServer.create(InetSocketAddress(6010), 0)

            httpServer.createContext("/stopbelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/movebelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6010"
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-1"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            testSubscriberBehaviour.eStartUnloadBehavior = {
                beltProxy.startUnloading()
            }

            testSubscriberBehaviour.eJobDoneBehavior = {
                beltProxy.markJobDone()
            }

            val unloadingStatus = mutableListOf<CompletableFuture<Boolean>>()
            testSubscriberBehaviour.isUnloadingBehavior = {
                unloadingStatus.add(CompletableFuture.completedFuture(it))
            }

            daprClient.publishEvent("pubsub", "eObjectValid", true).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "eStartUnload", 0).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "eJobDone", 0).block()

            Thread.sleep(3000)
            assert(unloadingStatus.size >= 2)
            assertEquals(true, unloadingStatus[0].get())
            assertEquals(false, unloadingStatus[1].get())
        }
    }

    @Test
    fun testArmPickupEventRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(30)) {
            val httpServer = HttpServer.create(InetSocketAddress(6011), 0)

            httpServer.createContext("/stopbelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/movebelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6011"
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-2"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            testSubscriberBehaviour.eStartUnloadBehavior = {
                beltProxy.startUnloading()
            }

            val pickupInitiationStatus = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eArmPickupBehavior = {
                pickupInitiationStatus.complete(true)
            }

            daprClient.publishEvent("pubsub", "eObjectValid", true).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "eStartUnload", 0).block()

            assertEquals(true, pickupInitiationStatus.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testErrorMessageRaising()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-3"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            val message = CompletableFuture<String>()
            testSubscriberBehaviour.eProcessMessageBehavior = {
                message.complete(it)
            }

            daprClient.publishEvent("pubsub", "eObjectValid", false).block()

            assertEquals("Belt error: Invalid object detected", message.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEScannedEventRaisingOnTransitionToTransporting()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-4"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            val eScannedEventReceived = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eScannedBehavior = {
                eScannedEventReceived.complete(true)
            }

            daprClient.publishEvent("pubsub", "eObjectValid", true).block()

            assertEquals(true, eScannedEventReceived.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEScannedEventRaisingOnTransitionToJobDone()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-5"))

            testSubscriberBehaviour.eJobDoneBehavior = {
                beltProxy.markJobDone()
            }

            val eScannedEventReceived = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eScannedBehavior = {
                eScannedEventReceived.complete(true)
            }

            daprClient.publishEvent("pubsub", "eJobDone", true).block()

            assertEquals(true, eScannedEventReceived.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEScannedEventRaisingOnTransitionToError()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(20)) {
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-6"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            val eScannedEventReceived = CompletableFuture<Boolean>()
            testSubscriberBehaviour.eScannedBehavior = {
                eScannedEventReceived.complete(true)
            }

            daprClient.publishEvent("pubsub", "eObjectValid", false).block()

            assertEquals(true, eScannedEventReceived.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun testEArmPickupRetry()
    {
        assertTimeoutPreemptively(Duration.ofSeconds(60)) {
            val httpServer = HttpServer.create(InetSocketAddress(6012), 0)

            httpServer.createContext("/stopbelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.createContext("/movebelt") { exchange ->
                exchange.use {
                    exchange.sendResponseHeaders(200, 0)
                }
            }
            httpServer.start()

            Services.baseUrl = "http://localhost:6012"
            val beltProxy = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId("bt-2"))

            testSubscriberBehaviour.eObjectValidBehavior = {
                beltProxy.markObjectValidity(it)
            }

            testSubscriberBehaviour.eStartUnloadBehavior = {
                beltProxy.startUnloading()
            }

            val pickupTries = AtomicInteger(0)
            testSubscriberBehaviour.eArmPickupBehavior = {
                pickupTries.incrementAndGet()
            }

            daprClient.publishEvent("pubsub", "eObjectValid", true).block()
            Thread.sleep(1000)
            daprClient.publishEvent("pubsub", "eStartUnload", 0).block()

            Thread.sleep(30000)
            assert(pickupTries.get() >= 2)
        }
    }

}
