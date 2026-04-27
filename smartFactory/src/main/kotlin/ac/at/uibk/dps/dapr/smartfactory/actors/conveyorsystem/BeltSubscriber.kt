package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class BeltSubscriber {
    private val actorId = System.getenv("ACTOR_ID") ?: "belt-0"
    private val proxy: BeltActor =
        ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId(actorId))

    @Topic(name = "eObjectValid", pubsubName = "pubsub")
    @PostMapping("/eObjectValid")
    fun setIsUnloading(@RequestBody event: CloudEvent<Boolean>) {
        proxy.markObjectValidity(event.data)
    }

    @Topic(name = "eStartUnload", pubsubName = "pubsub")
    @PostMapping("/eStartUnload")
    fun setIsUnloading() {
        proxy.startUnloading()
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun markJobDone() {
        proxy.markJobDone()
    }

    @Topic(name = "ePickedUp", pubsubName = "pubsub")
    @PostMapping("/ePickedUp")
    fun markPickedUp() {
        proxy.markPickedUp()
    }
}
