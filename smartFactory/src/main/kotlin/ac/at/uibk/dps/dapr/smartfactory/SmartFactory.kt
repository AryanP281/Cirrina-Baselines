package ac.at.uibk.dps.dapr.smartfactory

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SmartFactory {

  fun main(args: Array<String>) {
    println("Application started")
    runApplication<SmartFactory>(*args)
  }
}
