package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class MonitorSubscriber {

  private val actorId = System.getenv("ACTOR_ID") ?: "monitor-0"
  private val proxy: MonitorActor =
    ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun newProductComplete() {
    proxy.incrementProductsCompletedCount()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun markJobDone() {
    proxy.markJobDone()
  }
}
