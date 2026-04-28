package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "Sensor")
interface SensorActor {
  enum class States {
    IDLE,
    DETECTING,
    DETECTED,
  }

  enum class Types {
    START,
    END
  }

  @ActorMethod(name = "initialize") fun initialize()

  @ActorMethod(name = "setIsUnloading") fun setIsUnloading(isUnloading: Boolean)

  @ActorMethod(name = "setIsScanning") fun setIsScanning(isScanning: Boolean)

  @ActorMethod(name = "onBeamDetectionTimeout") fun onBeamDetectionTimeout() : Mono<Void>
}
