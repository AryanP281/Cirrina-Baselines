package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "Belt")
interface BeltActor {

    enum class States{
        LOADING,
        TRANSPORTING,
        UNLOADING,
        ERROR,
        JOB_DONE
    }

    @ActorMethod(name = "markObjectValidity")
    fun markObjectValidity(isValid: Boolean)

    @ActorMethod(name="startUnloading")
    fun startUnloading()

    @ActorMethod(name="markJobDone")
    fun markJobDone()

    @ActorMethod(name="markPickedUp")
    fun markPickedUp()
}
