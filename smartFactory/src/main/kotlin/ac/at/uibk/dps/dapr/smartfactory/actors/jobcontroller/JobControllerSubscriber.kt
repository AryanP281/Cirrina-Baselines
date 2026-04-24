package ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class JobControllerSubscriber {

  private val actorId = System.getenv("ACTOR_ID") ?: "jobcontroller-0"
  private val proxy: JobControllerActor =
    ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId))

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun newProductComplete() {
    proxy.markProductCompleted()
  }
}
