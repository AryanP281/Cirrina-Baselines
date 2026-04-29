package ac.at.uibk.dps.dapr.smartfactory.config

import ac.at.uibk.dps.dapr.smartfactory.actors.arm.ArmActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.CameraActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.SensorActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor.MessageProcessorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.monitor.MonitorActorImpl
import io.dapr.actors.runtime.ActorRuntime
import jakarta.annotation.PostConstruct
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Profile

@Profile("test")
@TestConfiguration
class TestActorsConfig {

    @PostConstruct
    fun registerActors() {
        ActorRuntime.getInstance().registerActor(JobControllerActorImpl::class.java)
        ActorRuntime.getInstance().registerActor(MessageProcessorImpl::class.java)
        ActorRuntime.getInstance().registerActor(MonitorActorImpl::class.java)
        ActorRuntime.getInstance().registerActor(SensorActorImpl::class.java)
        ActorRuntime.getInstance().registerActor(CameraActorImpl::class.java)
        ActorRuntime.getInstance().registerActor(BeltActorImpl::class.java)
        ActorRuntime.getInstance().registerActor(ArmActorImpl::class.java)
    }

}
