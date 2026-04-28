package ac.at.uibk.dps.dapr.smartfactory.config

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import kotlin.concurrent.atomics.AtomicReference

@Profile("test")
@Component
class TestSubscriberBehaviour {

    @Volatile
    var eProductCompleteBehavior : () -> Unit = {}

    @Volatile
    var eJobDoneBehavior : () -> Unit = {}

    @Volatile
    var eProcessMessageBehavior : (String) -> Unit = {}
}
