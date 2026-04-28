package ac.at.uibk.dps.dapr.smartfactory.config

import ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor.MessageProcessorActor
import ac.at.uibk.dps.dapr.smartfactory.actors.monitor.MonitorActor
import io.dapr.Topic
import io.dapr.client.domain.CloudEvent
import org.springframework.context.annotation.Profile
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@Profile("test")
@RestController
class TestSubscriber(
    val behavior : TestSubscriberBehaviour
)
{
    @Topic(name = "eProductComplete", pubsubName = "pubsub")
    @PostMapping("/eProductComplete")
    fun newProductComplete() : ResponseEntity<Unit> {
        behavior.eProductCompleteBehavior()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eProcessMessage", pubsubName = "pubsub")
    @PostMapping("/eProcessMessage")
    fun processMessage(@RequestBody event: CloudEvent<Map<String, String>>) : ResponseEntity<Unit>
    {
        behavior.eProcessMessageBehavior(event.data["msg"] ?: "")
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun markJobDone() : ResponseEntity<Unit> {
        behavior.eJobDoneBehavior()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eScanned", pubsubName = "pubsub")
    @PostMapping("/eScanned")
    fun incrementScannedCount() : ResponseEntity<Unit> {
        behavior.eScannedBehavior()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
    @PostMapping("/eAssemblyComplete")
    fun incrementAssembledCount() : ResponseEntity<Unit> {
        behavior.eAssemblyCompleteBehavior()
        return ResponseEntity.ok().build()
    }
}
