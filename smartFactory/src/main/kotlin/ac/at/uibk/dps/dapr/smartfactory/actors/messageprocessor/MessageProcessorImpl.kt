package ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor

import ac.at.uibk.dps.dapr.smartfactory.services.MessageProcessingRequest
import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import org.slf4j.LoggerFactory

class MessageProcessorImpl(
  runtimeContext: ActorRuntimeContext<MessageProcessorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), MessageProcessorActor {
  private var currentActiveState: MessageProcessorActor.States = MessageProcessorActor.States.IDLE

  private val logger = LoggerFactory.getLogger(MessageProcessorImpl::class.java)

  private fun transition(targetState: MessageProcessorActor.States, data: Any? = null) {
    if (currentActiveState == MessageProcessorActor.States.JOB_DONE) return

    when (targetState) {
      MessageProcessorActor.States.IDLE -> {
        currentActiveState = MessageProcessorActor.States.IDLE
      }
      MessageProcessorActor.States.PROCESS -> {
        if (currentActiveState == MessageProcessorActor.States.IDLE) {
          currentActiveState = MessageProcessorActor.States.PROCESS
          processState(data as String)
        }
      }
      MessageProcessorActor.States.JOB_DONE -> {
        if (currentActiveState == MessageProcessorActor.States.IDLE)
          currentActiveState = MessageProcessorActor.States.JOB_DONE
      }
    }
  }

  override fun processMessage(message: String) {
    if(currentActiveState == MessageProcessorActor.States.IDLE)
      transition(MessageProcessorActor.States.PROCESS, message)
  }

  override fun markJobDone() {
    if(currentActiveState == MessageProcessorActor.States.IDLE)
    transition(MessageProcessorActor.States.JOB_DONE)
  }

  private fun processState(msg: String) {
    Services.processEmail(MessageProcessingRequest(msg)).block()

    transition(MessageProcessorActor.States.IDLE)
  }
}
