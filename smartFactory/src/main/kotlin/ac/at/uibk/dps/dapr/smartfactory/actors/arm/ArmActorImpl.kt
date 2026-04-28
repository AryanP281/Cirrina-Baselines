package ac.at.uibk.dps.dapr.smartfactory.actors.arm

import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActor
import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import org.checkerframework.checker.units.qual.A
import java.time.Duration

class ArmActorImpl(
    runtimeContext: ActorRuntimeContext<ArmActorImpl>,
    id : ActorId
) : AbstractActor(runtimeContext, id), ArmActor
{
    private val partsPerProduct = 2
    private var currActiveState = ArmActor.States.IDLE
    private var pickupSuccess = true
    private var errorMsg = ""
    private var partsAssembled = 0

    private val daprClient = DaprClientBuilder().build()

    private fun transition(targetState: ArmActor.States, data: Any? = null) {
        when(targetState) {
            ArmActor.States.IDLE -> {
                if(currActiveState == ArmActor.States.RETURN) {
                    currActiveState = targetState
                    idleState()
                }
            }

            ArmActor.States.PICKUP -> {
                if(currActiveState == ArmActor.States.IDLE)
                {
                    currActiveState = targetState
                    pickupState()
                }
            }

            ArmActor.States.ASSEMBLE -> {
                if(currActiveState == ArmActor.States.PICKUP || currActiveState == ArmActor.States.ERROR)
                {
                    currActiveState = targetState
                    assembleState()
                }
            }

            ArmActor.States.ERROR -> {
                if(currActiveState == ArmActor.States.PICKUP || currActiveState == ArmActor.States.ASSEMBLE)
                {
                    currActiveState = targetState
                    errorState()
                }
            }

            ArmActor.States.RETURN -> {
                if(currActiveState == ArmActor.States.ASSEMBLE || currActiveState == ArmActor.States.ERROR)
                {
                    currActiveState = targetState
                    returnState()
                }
            }

            ArmActor.States.JOB_DONE -> {
                currActiveState = targetState
            }
        }
    }

    override fun initialize()
    {
        idleState()
    }

    override fun initiatePickup()
    {
        if(currActiveState != ArmActor.States.JOB_DONE)
            transition(ArmActor.States.PICKUP)
    }

    override fun updatePickupStatus(pickupStatus: Boolean) {
        if(currActiveState != ArmActor.States.ERROR)
            this.pickupSuccess = pickupStatus
    }

    override fun markJobDone() {
        transition(ArmActor.States.JOB_DONE)
    }

    private fun idleState()
    {
        if(!pickupSuccess)
            transition(ArmActor.States.PICKUP)
    }

    private fun pickupState()
    {
        //Invoke arm pickup
        pickupSuccess = Services.pickUp().block()?.success ?: false

        if(pickupSuccess)
        {
            //Raise ePickedUp
            daprClient.publishEvent("pubsub", "ePickedUp", mapOf<String,Any>()).subscribe()

            transition(ArmActor.States.ASSEMBLE)
        }
        else
        {
            errorMsg = "Pickup failed..."
            transition(ArmActor.States.ERROR)
        }
    }

    private fun assembleState()
    {
        //Invoke Assemble
        val assemblyStatus = Services.assemble().block()?.success ?: false

        if(assemblyStatus)
        {
            partsAssembled++
            daprClient.publishEvent("pubsub", "eAssemblyComplete", mapOf<String,Any>()).subscribe()
            transition(ArmActor.States.RETURN)
        }
        else
        {
            errorMsg = "Assemble failed..."
            transition(ArmActor.States.ERROR)
        }
    }

    private fun errorState()
    {
        //Raise eProcessMessage
        daprClient.publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Fatal robotic arm failure: $errorMsg")).subscribe()

        //Starting timer eRetry
        registerActorTimer("eRetryTimer", "retryTimeout", null, Duration.ofSeconds(10), Duration.ofSeconds(10))
    }

    private fun returnState()
    {
        //Invoke return to start action
        Services.returnToStart().block()

        if(partsAssembled >= partsPerProduct)
        {
            partsAssembled = 0
            daprClient.publishEvent("pubsub", "eProductComplete", mapOf<String,Any>()).subscribe()
        }

        transition(ArmActor.States.IDLE)
    }

    private fun retryTimeout()
    {
        if(pickupSuccess)
            transition(ArmActor.States.ASSEMBLE)
        else
            transition(ArmActor.States.RETURN)
    }
}
