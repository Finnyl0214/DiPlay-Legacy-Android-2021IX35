package com.shilapi.xcertplay.bluetooth

import android.content.Context
import android.content.pm.PackageManager
import com.shilapi.xcertplay.bluetooth.ix35.Ix35AnwSocketStream
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import java.io.IOException

/**
 * ix35 factory Bluetooth data backend.
 *
 * The factory ANW service exposes an iAP2 socket independently of Android's
 * BluetoothAdapter. It cannot enumerate bonded phones or expose the local
 * controller address through the currently verified API, so callers must
 * supply the already-paired iPhone MAC explicitly.
 */
class Ix35AnwBluetoothAdapter(private val context: Context) : CarPlayBluetooth {
    @Volatile
    private var diagnosticSink: (String) -> Unit = {}

    /** Attach the current CarPlay session logger without coupling the transport to an Activity. */
    fun setDiagnosticSink(sink: (String) -> Unit) {
        diagnosticSink = sink
    }

    companion object {
        const val SERVICE_PACKAGE = "com.anwbtsdk.service"
        private val ADDRESS_PATTERN = Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")
    }

    override fun isSupported(): Boolean = runCatching {
        context.packageManager.getPackageInfo(SERVICE_PACKAGE, 0)
        true
    }.getOrDefault(false)

    // ANW's verified API exposes SocketInit, not a general radio power switch.
    // Do not report an Android framework power state for this vendor transport.
    override fun isEnabled(): Boolean = isSupported()
    override fun enable(): Boolean = isSupported()
    override fun getAdapterName(): String = "ix35 ANW Socket"

    override fun getPairedDevices(): List<CarPlayBluetoothDevice> = emptyList()
    override fun getConnectedDevice(): CarPlayBluetoothDevice? = null

    // The currently verified ANW SDK calls do not establish the local BD_ADDR.
    override fun localAddress(context: Context): String? = null

    override fun openRfcommStream(deviceAddress: String, uuid: String): BlockingDuplexByteStream {
        if (!ADDRESS_PATTERN.matches(deviceAddress)) {
            throw IOException("Invalid paired iPhone Bluetooth address")
        }
        return Ix35AnwSocketStream.connect(context.applicationContext, deviceAddress, uuid) { message ->
            runCatching { diagnosticSink(message) }
        }
    }
}
