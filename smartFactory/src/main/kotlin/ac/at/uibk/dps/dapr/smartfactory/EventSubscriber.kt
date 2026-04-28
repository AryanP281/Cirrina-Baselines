package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.arm.ArmActor
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActor
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.CameraActor
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.SensorActor
import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActor
import ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor.MessageProcessorActor
import ac.at.uibk.dps.dapr.smartfactory.actors.monitor.MonitorActor
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class EventSubscriber {
    private val role = System.getenv("ROLE")
    private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"

    val actorProxy = when(role) {
        "jobcontroller" -> ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId))
        "messageprocessor" -> ActorProxyBuilder(MessageProcessorActor::class.java, ActorClient()).build(ActorId(actorId))
        "monitor" -> ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))
        "startsensor", "endsensor" -> ActorProxyBuilder(SensorActor::class.java, ActorClient()).build(ActorId(actorId))
        "camera" -> ActorProxyBuilder(CameraActor::class.java, ActorClient()).build(ActorId(actorId))
        "belt" -> ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId(actorId))
        else -> ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))
    }

    @Topic(name = "eProductComplete", pubsubName = "pubsub")
    @PostMapping("/eProductComplete")
    fun newProductComplete() {
        when(actorProxy){
            is JobControllerActor -> (actorProxy as JobControllerActor).markProductCompleted()
            is MonitorActor -> (actorProxy as MonitorActor).incrementProductsCompletedCount()
        }
    }

    @Topic(name = "eProcessMessage", pubsubName = "pubsub")
    @PostMapping("/eProcessMessage")
    fun processMessage(@RequestBody event: CloudEvent<Map<String, String>>)
    {
        if(actorProxy is MessageProcessorActor)
            (actorProxy as MessageProcessorActor).processMessage(event.data["msg"] ?: "")
    }

    @Topic(name = "eScanned", pubsubName = "pubsub")
    @PostMapping("/monitor/eScanned")
    fun incrementScannedCount() {
        when(actorProxy) {
            is MonitorActor -> (actorProxy as MonitorActor).markScanned()
        }
    }

    @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
    @PostMapping("/monitor/eAssemblyComplete")
    fun incrementAssembledCount() {
        when(actorProxy) {
            is MonitorActor -> (actorProxy as MonitorActor).markAssembled()
        }
    }

    @Topic(name = "isUnloading", pubsubName = "pubsub")
    @PostMapping("/isUnloading")
    fun setIsUnloading(@RequestBody event: CloudEvent<Boolean>) {
        when(actorProxy) {
            is SensorActor -> (actorProxy as SensorActor).setIsUnloading(event.data)
        }
    }

    @Topic(name = "isScanning", pubsubName = "pubsub")
    @PostMapping("/isScanning")
    fun setIsScanning(@RequestBody event: CloudEvent<Boolean>) {
        when(actorProxy) {
            is SensorActor -> (actorProxy as SensorActor).setIsScanning(event.data)
        }
    }

    @Topic(name = "eStartScan", pubsubName = "pubsub")
    @PostMapping("/eStartScan")
    fun startScan() {
        when(actorProxy) {
            is CameraActor -> (actorProxy as CameraActor).startScan()
        }
    }

    @Topic(name = "eObjectValid", pubsubName = "pubsub")
    @PostMapping("/eObjectValid")
    fun setObjectValidity(@RequestBody event: CloudEvent<Boolean>) {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).markObjectValidity(event.data)
        }
    }

    @Topic(name = "eStartUnload", pubsubName = "pubsub")
    @PostMapping("/eStartUnload")
    fun startUnloading() {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).startUnloading()
        }
    }

    @Topic(name = "ePickedUp", pubsubName = "pubsub")
    @PostMapping("/ePickedUp")
    fun markPickedUp() {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).markPickedUp()
        }
    }

    @Topic(name = "eArmPickup", pubsubName = "pubsub")
    @PostMapping("/eArmPickup")
    fun pickup() {
        when(actorProxy) {
            is ArmActor -> (actorProxy as ArmActor).initiatePickup()
        }
    }

    @Topic(name = "eUpdatePickupSuccessStatus", pubsubName = "pubsub")
    @PostMapping("/eUpdatePickupSuccessStatus")
    fun updatePickupStatus(@RequestBody event: CloudEvent<Boolean>) {
        when(actorProxy) {
            is ArmActor -> (actorProxy as ArmActor).updatePickupStatus(event.data)
        }
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun markJobDone() {
        when(actorProxy) {
            is MessageProcessorActor -> (actorProxy as MessageProcessorActor).markJobDone()
            is MonitorActor -> (actorProxy as MonitorActor).markJobDone()
            is BeltActor -> (actorProxy as BeltActor).markJobDone()
            is ArmActor -> (actorProxy as ArmActor).markJobDone()
        }
    }
}
