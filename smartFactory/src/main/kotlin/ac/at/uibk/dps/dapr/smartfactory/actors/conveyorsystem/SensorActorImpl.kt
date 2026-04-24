package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.time.Duration
import reactor.core.publisher.Mono

class SensorActorImpl(runtimeContext: ActorRuntimeContext<SensorActorImpl>, id: ActorId, private val sensorType : SensorActor.Types) :
  AbstractActor(runtimeContext, id), SensorActor {
  var currentActiveState: SensorActor.States = SensorActor.States.IDLE
  var isUnloading: Boolean = false
  var isScanning: Boolean = false
  var isBeamInterrupted: Boolean = false

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: SensorActor.States, data: Any? = null) {
    when (targetState) {
      SensorActor.States.IDLE -> {
        if(currentActiveState == SensorActor.States.DETECTED) {
          currentActiveState = SensorActor.States.IDLE
          idleState()
        }
      }
      SensorActor.States.DETECTING -> {
        if (currentActiveState == SensorActor.States.IDLE) {
          currentActiveState = SensorActor.States.DETECTING
          detectingState()
        }
      }
      SensorActor.States.DETECTED -> {
        if(currentActiveState == SensorActor.States.DETECTING) {
          currentActiveState = SensorActor.States.DETECTED
          detectedState()
        }
      }
    }
  }

  override fun initialize() {
    idleState()
  }

  override fun setIsUnloading(isUnloading: Boolean) {
    this.isUnloading = isUnloading
  }

  override fun setIsScanning(isScanning: Boolean) {
    this.isScanning = isScanning
  }

  private fun idleState() {
    // Starting beam detection timer
    registerActorTimer(
      "beamDetectionTimeout",
      "onBeamDetectionTimeout",
      null,
      Duration.ofSeconds(1),
      Duration.ofSeconds(1),
    )
  }

  private fun detectingState() {
    //Invoke beam detection service
    isBeamInterrupted = Services.beamDetectionStart().block()?.interrupted ?: false

    if(isBeamInterrupted)
      transition(SensorActor.States.DETECTED)
    else
      transition(SensorActor.States.IDLE)
  }

  private fun detectedState() {
    if(sensorType == SensorActor.Types.START) {
      daprClient.publishEvent("pubsub", "eStartScan", null).subscribe()
    }
    else {
      daprClient.publishEvent("pubsub", "eStartUnload", null).subscribe()
    }

    transition(SensorActor.States.IDLE)
  }

  fun onBeamDetectionTimeout(state: Any?): Mono<Void> {
    if (!isUnloading && !isScanning) {
      transition(SensorActor.States.DETECTING)
      unregisterTimer("beamDetectionTimeout")
    }
    return Mono.empty()
  }
}
