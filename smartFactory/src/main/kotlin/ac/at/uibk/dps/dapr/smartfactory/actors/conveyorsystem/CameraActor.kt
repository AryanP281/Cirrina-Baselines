package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "Camera")
interface CameraActor {

    enum class States {
        IDLE,
        SCANNING
    }

    @ActorMethod(name = "startScan")
    fun startScan()
}
