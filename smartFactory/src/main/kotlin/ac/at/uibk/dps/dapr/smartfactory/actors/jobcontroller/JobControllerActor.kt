package ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "JobController")
interface JobControllerActor {
  enum class States {
    STARTING,
    RUNNING,
    JOB_DONE,
  }

  @ActorMethod(name = "initialize") fun initialize()

  @ActorMethod(name = "markProductCompleted") fun markProductCompleted()
}
