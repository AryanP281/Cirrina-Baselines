package ac.at.uibk.dps.dapr.smartfactory.actors.arm

import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActor
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class ArmSubscriber {
    private val actorId = System.getenv("ACTOR_ID") ?: "arm-0"
    private val proxy: ArmActor =
        ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))

    @Topic(name = "eArmPickup", pubsubName = "pubsub")
    @PostMapping("/eArmPickup")
    fun setIsUnloading() {
        proxy.initiatePickup()
    }

    @Topic(name = "eUpdatePickupSuccessStatus", pubsubName = "pubsub")
    @PostMapping("/eUpdatePickupSuccessStatus")
    fun updatePickupStatus(@RequestBody event: CloudEvent<Boolean>) {
        proxy.updatePickupStatus(event.data)
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun markJobDone() {
        proxy.markJobDone()
    }
}
