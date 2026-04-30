package ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem

import ac.at.uibk.dps.dapr.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import reactor.core.publisher.Mono
import java.time.Duration

class BeltActorImpl (
    runtimeContext: ActorRuntimeContext<BeltActorImpl>,
    id : ActorId
) : AbstractActor(runtimeContext, id), BeltActor
{

    private var currentActiveState : BeltActor.States = BeltActor.States.LOADING
    private var isUnloading = false

    private val daprClient = DaprClientBuilder().build()

    private fun transition(targetState: BeltActor.States, data: Any? = null) {

        when(targetState) {
            BeltActor.States.LOADING -> {
                if(currentActiveState == BeltActor.States.UNLOADING) {
                    //Exit actions
                    unregisterTimer("armPickupTimeout-${id}").block()

                    currentActiveState = targetState
                }
            }

            BeltActor.States.TRANSPORTING -> {
                if(currentActiveState == BeltActor.States.LOADING || currentActiveState == BeltActor.States.ERROR) {
                    //Exit actions
                    if(currentActiveState == BeltActor.States.LOADING)
                        daprClient.publishEvent("pubsub", "eScanned", mapOf<String,Any>()).subscribe()

                    currentActiveState = BeltActor.States.TRANSPORTING
                    transportingState()
                }
            }

            BeltActor.States.UNLOADING -> {
                if(currentActiveState == BeltActor.States.TRANSPORTING) {
                    //Exit actions
                    Services.stopBelt().block()

                    currentActiveState = BeltActor.States.UNLOADING
                    unloadingState()
                }
            }

            BeltActor.States.ERROR -> {
                if(currentActiveState == BeltActor.States.LOADING) {
                    //Exit actions
                    daprClient.publishEvent("pubsub", "eScanned", mapOf<String,Any>()).subscribe()

                    currentActiveState = BeltActor.States.ERROR
                    errorState()
                }
            }

            BeltActor.States.JOB_DONE -> {
                //Exit actions
                if(currentActiveState == BeltActor.States.LOADING)
                    daprClient.publishEvent("pubsub", "eScanned", mapOf<String,Any>()).subscribe()
                if(currentActiveState == BeltActor.States.UNLOADING)
                    unregisterTimer("armPickupTimeout-${id}").block()

                currentActiveState = BeltActor.States.JOB_DONE
            }

        }
    }

    private fun transportingState() {
        //Invoke MoveBelt Action
        Services.moveBelt().block()
    }

    private fun unloadingState() {
        //Starting timer for eArmPickup
        registerActorTimer("armPickupTimeout-${id}", "armPickupTimeout", 0, Duration.ofSeconds(0), Duration.ofSeconds(10)).subscribe()
    }

    private fun errorState()
    {
        //Raising message
        daprClient.publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Belt error: Invalid object detected")).subscribe()
    }

    override fun markObjectValidity(isValid: Boolean)
    {
        if(currentActiveState == BeltActor.States.LOADING)
        {
            if(isValid)
                transition(BeltActor.States.TRANSPORTING)
            else
                transition(BeltActor.States.ERROR)
        }
    }

    override fun startUnloading() {
        println(currentActiveState.name)
        if(currentActiveState == BeltActor.States.TRANSPORTING) {
            isUnloading = true
            daprClient.publishEvent("pubsub", "isUnloading", isUnloading).subscribe()
            transition(BeltActor.States.UNLOADING)
        }
    }

    override fun markJobDone()
    {
        if(currentActiveState == BeltActor.States.UNLOADING){
            isUnloading = false
            daprClient.publishEvent("pubsub", "isUnloading", isUnloading).subscribe()
        }

        transition(BeltActor.States.JOB_DONE)
    }

    override fun markPickedUp()
    {
        if(currentActiveState == BeltActor.States.UNLOADING)
        {
            isUnloading = false
            daprClient.publishEvent("pubsub", "isUnloading", isUnloading).subscribe()

            transition(BeltActor.States.LOADING)
        }
    }

    override fun armPickupTimeout() : Mono<Void>
    {
        //Raising eArmPickup
        daprClient.publishEvent("pubsub", "eArmPickup", mapOf<String,Any>()).subscribe()

        return Mono.empty()
    }


}
