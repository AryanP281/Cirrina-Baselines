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
class SensorActorSubscriber {
  private val actorId = System.getenv("ACTOR_ID") ?: "sensor-0"
  private val proxy: SensorActor =
    ActorProxyBuilder(SensorActor::class.java, ActorClient()).build(ActorId(actorId))

  @Topic(name = "isUnloading", pubsubName = "pubsub")
  @PostMapping("/isUnloading")
  fun setIsUnloading(@RequestBody event: CloudEvent<Boolean>) {
    proxy.setIsUnloading(event.data)
  }

  @Topic(name = "isScanning", pubsubName = "pubsub")
  @PostMapping("/isScanning")
  fun setIsScanning(@RequestBody event: CloudEvent<Boolean>) {
    proxy.setIsScanning(event.data)
  }
}
