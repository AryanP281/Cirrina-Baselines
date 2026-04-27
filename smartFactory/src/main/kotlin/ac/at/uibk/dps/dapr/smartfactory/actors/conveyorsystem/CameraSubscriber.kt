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
class CameraSubscriber {
    private val actorId = System.getenv("ACTOR_ID") ?: "camera-0"
    private val proxy: CameraActor =
        ActorProxyBuilder(CameraActor::class.java, ActorClient()).build(ActorId(actorId))

    @Topic(name = "eStartScan", pubsubName = "pubsub")
    @PostMapping("/eStartScan")
    fun setIsUnloading() {
        proxy.startScan()
    }

}
