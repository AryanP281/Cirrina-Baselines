package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import ac.at.uibk.dps.dapr.smartfactory.services.Services
import ac.at.uibk.dps.dapr.smartfactory.services.StatisticsRequest
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import org.slf4j.LoggerFactory

class MonitorActorImpl(runtimeContext: ActorRuntimeContext<MonitorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), MonitorActor {

  private var currentActiveState: MonitorActor.States = MonitorActor.States.MONITORING
  private var nScans = 0
  private var nAssemblies = 0
  private var productsCompleted = 0
  private var jobDone = false

  private val logger = LoggerFactory.getLogger(MonitorActorImpl::class.java)

  override fun markScanned() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nScans += 1

      //Invoke SendStatistics service
      logger.info("Statistics due to scan")
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).block()
    }
  }

  override fun markAssembled() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nAssemblies += 1

      //Invoke SendStatistics service
      logger.info("Statistics due to assembly")
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).block()
    }
  }

  override fun incrementProductsCompletedCount() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      productsCompleted += 1
      logger.info("$productsCompleted products completed")

      //Invoke SendStatistics service
      logger.info("Statistics due to product completion")
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).block()
    }
  }

  override fun markJobDone() {
    if(currentActiveState == MonitorActor.States.MONITORING) {
      currentActiveState = MonitorActor.States.JOB_DONE
      jobDone = true

      //Invoke SendStatistics service
      logger.info("Statistics due to job done")
      Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone)).block()
    }
  }
}
