package ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class MessageProcessorSubscriber {
  private val actorId = System.getenv("ACTOR_ID") ?: "messageprocessor-0"
  private val proxy: MessageProcessorActor =
    ActorProxyBuilder(MessageProcessorActor::class.java, ActorClient()).build(ActorId(actorId))

  @Topic(name = "eProcessMessage", pubsubName = "pubsub")
  @PostMapping("/eProcessMessage")
  fun processMessage(@RequestBody event: CloudEvent<Map<String, String>>) {
    proxy.processMessage(event.data["msg"] ?: "")
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun markJobDone() {
    proxy.markJobDone()
  }
}
