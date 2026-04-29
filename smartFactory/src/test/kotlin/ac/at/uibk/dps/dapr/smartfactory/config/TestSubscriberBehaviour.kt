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

    @Volatile
    var eScannedBehavior : () -> Unit = {}

    @Volatile
    var eAssemblyCompleteBehavior : () -> Unit = {}

    @Volatile
    var eStartScanBehavior : () -> Unit = {}

    @Volatile
    var eStartUnloadBehavior : () -> Unit = {}

    @Volatile
    var isScanningBehavior : (Boolean) -> Unit = {}

    @Volatile
    var eObjectValidBehavior : (Boolean) -> Unit = {}

    @Volatile
    var isUnloadingBehavior : (Boolean) -> Unit = {}

    @Volatile
    var ePickedUpBehavior : () -> Unit = {}

    @Volatile
    var eArmPickupBehavior : () -> Unit = {}
}
