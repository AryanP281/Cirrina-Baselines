package ac.at.uibk.dps.dapr.smartfactory

import ac.at.uibk.dps.dapr.smartfactory.actors.arm.ArmActor
import ac.at.uibk.dps.dapr.smartfactory.actors.arm.ArmActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.BeltActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.CameraActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.SensorActor
import ac.at.uibk.dps.dapr.smartfactory.actors.conveyorsystem.SensorActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActor
import ac.at.uibk.dps.dapr.smartfactory.actors.jobcontroller.JobControllerActorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.messageprocessor.MessageProcessorImpl
import ac.at.uibk.dps.dapr.smartfactory.actors.monitor.MonitorActorImpl
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClientBuilder
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SmartFactory

fun main(args: Array<String>)
{
  val role = System.getenv("ROLE")
  val actorId = System.getenv("ACTOR_ID") ?: "actor-0"

  println(role)
  when(role) {
    "jobcontroller" -> ActorRuntime.getInstance().registerActor(JobControllerActorImpl::class.java)
    "monitor" -> ActorRuntime.getInstance().registerActor(MonitorActorImpl::class.java)
    "messageprocessor" -> ActorRuntime.getInstance().registerActor(MessageProcessorImpl::class.java)
    "sensor" -> ActorRuntime.getInstance().registerActor(SensorActorImpl::class.java)
    "camera" -> ActorRuntime.getInstance().registerActor(CameraActorImpl::class.java)
    "belt" -> ActorRuntime.getInstance().registerActor(BeltActorImpl::class.java)
    "arm" -> ActorRuntime.getInstance().registerActor(ArmActorImpl::class.java)
    else -> println("ERROR: Unknown role $role")
  }

  runApplication<SmartFactory>(*args)

  when(role) {
    "jobcontroller" -> ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId)).initialize()
    "sensor" -> ActorProxyBuilder(SensorActor::class.java, ActorClient()).build(ActorId(actorId)).initialize()
    "arm" -> ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId)).initialize()
  }
}

