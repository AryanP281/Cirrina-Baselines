package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "Monitor")
interface MonitorActor {

  enum class States {
    MONITORING,
    JOB_DONE,
  }

  @ActorMethod(name = "markScanned") fun markScanned()

  @ActorMethod(name = "markAssembled") fun markAssembled()

  @ActorMethod(name = "incrementProductsCompletedCount") fun incrementProductsCompletedCount()

  @ActorMethod(name = "markJobDone") fun markJobDone()
}
