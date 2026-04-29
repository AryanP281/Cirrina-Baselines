package ac.at.uibk.dps.dapr.smartfactory.actors.arm

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name="arm")
interface ArmActor {
    enum class States {
        IDLE,
        ERROR,
        PICKUP,
        ASSEMBLE,
        RETURN,
        JOB_DONE
    }

    @ActorMethod(name = "initialize")
    fun initialize()

    @ActorMethod(name = "initiatePickup")
    fun initiatePickup()

    @ActorMethod(name = "updatePickupStatus")
    fun updatePickupStatus(pickupStatus : Boolean)

    @ActorMethod(name="markJobDone")
    fun markJobDone()

    @ActorMethod(name="retryTimeout")
    fun retryTimeout() : Mono<Void>
}
