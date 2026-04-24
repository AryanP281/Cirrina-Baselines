package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActor
import ac.at.uibk.dps.dapr.smartfactory.services.PhotoScanResponse
import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class CameraActorImpl(
    runtimeContext : ActorRuntimeContext<CameraActorImpl>,
    id : ActorId
) : AbstractActor(runtimeContext, id), CameraActor {

    private var currentActiveState : CameraActor.States = CameraActor.States.IDLE
    private var isScanning: Boolean = false

    private val daprClient = DaprClientBuilder().build()

    private fun transition(targetState: CameraActor.States, data: Any? = null) {
        when(targetState) {
            CameraActor.States.IDLE -> {
                if(currentActiveState == CameraActor.States.SCANNING) {
                    currentActiveState = CameraActor.States.IDLE
                }
            }
            CameraActor.States.SCANNING -> {
                if(currentActiveState == CameraActor.States.IDLE) {
                    currentActiveState = CameraActor.States.SCANNING
                    scanningState()
                }
            }
        }
    }

    override fun startScan()
    {
        if(currentActiveState == CameraActor.States.IDLE)
        {
            isScanning = true
            daprClient.publishEvent("pubsub", "isScanning", isScanning).subscribe()
        }
    }

    private fun scanningState() {
        Services.takePhoto().block()

        val photoScanResponse : PhotoScanResponse = Services.scanPhoto().block() ?: PhotoScanResponse(false)

        isScanning = false
        daprClient.publishEvent("pubsub", "isScanning", isScanning).subscribe()

        if(photoScanResponse.validObject)
            daprClient.publishEvent("pubsub", "eObjectValid", true).subscribe()
        else
            daprClient.publishEvent("pubsub", "eObjectValid", false).subscribe()

        transition(CameraActor.States.IDLE)
    }
}
