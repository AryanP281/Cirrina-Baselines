package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import ac.at.uibk.dps.dapr.smartfactory.services.Services
import ac.at.uibk.dps.dapr.smartfactory.services.StatisticsRequest
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext

class MonitorActorImpl(runtimeContext: ActorRuntimeContext<MonitorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), MonitorActor {

  private var currentActiveState: MonitorActor.States = MonitorActor.States.MONITORING
  private var nScans = 0
  private var nAssemblies = 0
  private var productsCompleted = 0
  private var jobDone = false

  override fun markScanned() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nScans++

      //Invoke SendStatistics service
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).subscribe()
    }
  }

  override fun markAssembled() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nAssemblies++

      //Invoke SendStatistics service
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).subscribe()
    }
  }

  override fun incrementProductsCompletedCount() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      productsCompleted++

      //Invoke SendStatistics service
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).subscribe()
    }
  }

  override fun markJobDone() {
    currentActiveState = MonitorActor.States.JOB_DONE
    jobDone = true

    //Invoke SendStatistics service
    Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).subscribe()
  }
}
