package ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "MessageProcessor")
interface MessageProcessorActor {
  enum class States {
    IDLE,
    PROCESS,
    JOB_DONE,
  }

  @ActorMethod(name = "processMessage") fun processMessage(message: String)

  @ActorMethod(name = "markJobDone") fun markJobDone()
}
