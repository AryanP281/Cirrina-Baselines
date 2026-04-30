package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.time.Duration
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicBoolean

class SensorActorImpl(runtimeContext: ActorRuntimeContext<SensorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), SensorActor {
  private var currentActiveState: SensorActor.States = SensorActor.States.IDLE
  private var isUnloading: Boolean = false
  private var isScanning: Boolean = false
  private var isBeamInterrupted: Boolean = false
  private val sensorType : SensorActor.Types = if (id.toString().startsWith("start")) SensorActor.Types.START else SensorActor.Types.END

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

  override fun onBeamDetectionTimeout(): Mono<Void> {
    if(currentActiveState == SensorActor.States.IDLE) {
      if (!isUnloading && !isScanning) {
        transition(SensorActor.States.DETECTING)
      }
      else
        startBeamDetectionTimer()
    }

    return Mono.empty()
  }

  private fun idleState() {
    // Starting beam detection timer
    startBeamDetectionTimer()
  }

  private fun detectingState() {
    //Invoke beam detection service
    isBeamInterrupted = when(sensorType) {
      SensorActor.Types.START -> Services.beamDetectionStart().block()?.interrupted ?: false
      SensorActor.Types.END -> Services.beamDetectionEnd().block()?.interrupted ?: false
    }

    if(isBeamInterrupted)
      transition(SensorActor.States.DETECTED)
    else
      transition(SensorActor.States.IDLE)
  }

  private fun detectedState() {
    if(sensorType == SensorActor.Types.START) {
      daprClient.publishEvent("pubsub", "eStartScan", mapOf<String,Any>()).subscribe()
    }
    else {
      daprClient.publishEvent("pubsub", "eStartUnload", mapOf<String,Any>()).subscribe()
    }
    transition(SensorActor.States.IDLE)
  }

  private fun startBeamDetectionTimer()
  {
    registerActorTimer(
      "beamDetectionTimeout-${id}",
      "onBeamDetectionTimeout",
      emptyMap<String,Any>(),
      Duration.ofSeconds(1),
      Duration.ofMillis(-1),
    ).subscribe()
  }
}
