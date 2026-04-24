package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext

class MonitorActorImpl(runtimeContext: ActorRuntimeContext<MonitorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), MonitorActor {

  private var currentActiveState: MonitorActor.States = MonitorActor.States.MONITORING
  private var nScans = 0
  private var nAssemblies = 0
  private var productsCompleted = 0

  override fun markScanned() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nScans++

      // TODO: Invoke SendStatistics service
    }
  }

  override fun markAssembled() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nAssemblies++

      // TODO: Invoke SendStatistics service
    }
  }

  override fun incrementProductsCompletedCount() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      productsCompleted++

      // TODO: Invoke SendStatistics service
    }
  }

  override fun markJobDone() {
    currentActiveState = MonitorActor.States.JOB_DONE
  }
}
