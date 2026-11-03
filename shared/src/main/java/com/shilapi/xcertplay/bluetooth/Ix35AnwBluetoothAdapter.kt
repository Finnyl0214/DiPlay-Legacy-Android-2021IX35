package com.shilapi.xcertplay.bluetooth

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import android.os.Looper
import android.os.Parcel
import android.os.RemoteException
import android.util.Log
import com.shilapi.xcertplay.bluetooth.ix35.Ix35AnwSocketStream
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * ix35 factory Bluetooth backend.
 *
 * The Android BluetoothAdapter is unavailable on this head unit, but its private
 * ANW service can report the current HFP phone and provide the iAP2 socket.
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
        private const val SERVICE_CLASS = "com.anwbtsdk.service.AnwBTSdkService"
        private const val SERVICE_DESCRIPTOR = "com.anwbtsdk.service.IAnwPhoneLink"

        // Verified against the ix35 factory AIDL Stub and the user's v2.3/v2.7 device logs.
        private const val TRANSACTION_GET_BT_POWER_STATUS = 3
        private const val TRANSACTION_GET_REMOTE_NAME = 8
        private const val TRANSACTION_GET_CONNECT_STATUS = 23
        private const val TRANSACTION_GET_CURRENT_HF_DEVICE_INFO = 25
        private const val TRANSACTION_GET_HFP_MAX_CONNECTION = 240

        private const val HFP_PROFILE = 0
        private const val CONNECTED_STATUS = 1
        private const val BINDER_TIMEOUT_MS = 2500L
        private const val DEFAULT_HFP_SLOTS = 2
        private const val MAX_HFP_SLOTS = 4
        private val ADDRESS_PATTERN = Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")
        private const val TAG = "Ix35AnwBluetoothAdapter"
    }

    override fun isSupported(): Boolean = runCatching {
        context.packageManager.getPackageInfo(SERVICE_PACKAGE, 0)
        true
    }.getOrDefault(false)

    /** Reads the vendor radio state; this is not inferred from service-package presence. */
    override fun isEnabled(): Boolean = readRadioPowerState() == true

    /** Null means the Binder query was unavailable (including a main-thread call). */
    fun readRadioPowerState(): Boolean? =
        withAnwBinder("radio-status") { remote ->
            transactInt(remote, TRANSACTION_GET_BT_POWER_STATUS) == 1
        }

    // Power-on remains user/system controlled; this backend does not silently toggle the radio.
    override fun enable(): Boolean = isEnabled()
    override fun getAdapterName(): String = "ix35 ANW Socket"

    /**
     * The verified API returns the currently connected HFP phone, not the full bond database.
     * Expose that connected phone as the only known paired-device candidate.
     */
    override fun getPairedDevices(): List<CarPlayBluetoothDevice> =
        getConnectedDevice()?.let { listOf(it) } ?: emptyList()

    override fun getConnectedDevice(): CarPlayBluetoothDevice? {
        return withAnwBinder("connected-hfp-phone") { remote ->
            val reportedMax = runCatching {
                transactInt(remote, TRANSACTION_GET_HFP_MAX_CONNECTION)
            }.getOrDefault(DEFAULT_HFP_SLOTS)
            val slotCount = reportedMax.coerceIn(1, MAX_HFP_SLOTS)

            for (index in 0 until slotCount) {
                val status = transactInt(remote, TRANSACTION_GET_CONNECT_STATUS) {
                    writeInt(HFP_PROFILE)
                    writeInt(index)
                }
                report("event=hfp-slot-status index=$index connected=${status == CONNECTED_STATUS}")
                if (status != CONNECTED_STATUS) continue

                // AIDL out parameter: write array length (1), then index; reply is
                // result int followed by the one-element String[] output.
                val addressOut = arrayOfNulls<String>(1)
                val result = transactInt(
                    remote,
                    TRANSACTION_GET_CURRENT_HF_DEVICE_INFO,
                    write = {
                        writeInt(addressOut.size)
                        writeInt(index)
                    },
                    readTail = { reply -> reply.readStringArray(addressOut) },
                )
                val address = normalizeAddress(addressOut[0])
                if (result != 0 || address == null) {
                    report("event=hfp-device-info-invalid index=$index result=$result addressValid=${address != null}")
                    continue
                }

                val name = runCatching {
                    transactString(remote, TRANSACTION_GET_REMOTE_NAME) { writeString(address) }
                }.getOrNull()?.takeIf { it.isNotBlank() } ?: "iPhone"
                report("event=hfp-device-discovered source=ANW-HFP index=$index nameAvailable=${name != "iPhone"}")
                return@withAnwBinder CarPlayBluetoothDevice(name, address, true)
            }
            report("event=hfp-device-not-connected slots=$slotCount")
            null
        }
    }

    // The ANW queries above expose the remote iPhone address, not the head unit's local BD_ADDR.
    override fun localAddress(context: Context): String? = null

    override fun openRfcommStream(deviceAddress: String, uuid: String): BlockingDuplexByteStream {
        val address = normalizeAddress(deviceAddress)
            ?: throw IOException("Invalid paired iPhone Bluetooth address")
        return Ix35AnwSocketStream.connect(context.applicationContext, address, uuid) { message ->
            runCatching { diagnosticSink(message) }
        }
    }

    private fun normalizeAddress(value: String?): String? {
        val candidate = value?.trim()?.uppercase()
        return candidate?.takeIf { ADDRESS_PATTERN.matches(it) && it != "00:00:00:00:00:00" }
    }

    /**
     * ANW ServiceConnection callbacks are delivered on the main looper on API 19.
     * All synchronous Binder reads must therefore be made from a worker thread.
     */
    private fun <T> withAnwBinder(label: String, block: (IBinder) -> T): T? {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            report("event=binder-query-skipped reason=main-thread query=$label")
            return null
        }
        val appContext = context.applicationContext
        val latch = CountDownLatch(1)
        var remote: IBinder? = null
        val connection = object : ServiceConnection {
            override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
                remote = service
                latch.countDown()
            }

            override fun onServiceDisconnected(name: ComponentName?) {
                remote = null
            }
        }
        val intent = Intent().setComponent(ComponentName(SERVICE_PACKAGE, SERVICE_CLASS))
        val bound = try {
            appContext.bindService(intent, connection, Context.BIND_AUTO_CREATE)
        } catch (error: Exception) {
            report("event=binder-bind-failed query=$label error=${error.javaClass.simpleName}")
            return null
        }
        if (!bound) {
            report("event=binder-bind-failed query=$label reason=bindService-false")
            return null
        }
        try {
            if (!latch.await(BINDER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                report("event=binder-bind-timeout query=$label")
                return null
            }
            val target = remote
            if (target == null || !target.isBinderAlive) {
                report("event=binder-unavailable query=$label")
                return null
            }
            return block(target)
        } catch (error: Exception) {
            report("event=binder-query-failed query=$label error=${error.javaClass.simpleName}")
            return null
        } finally {
            runCatching { appContext.unbindService(connection) }
        }
    }

    private fun transactInt(
        remote: IBinder,
        code: Int,
        write: Parcel.() -> Unit = {},
        readTail: (Parcel) -> Unit = {},
    ): Int {
        val data = Parcel.obtain()
        val reply = Parcel.obtain()
        try {
            data.writeInterfaceToken(SERVICE_DESCRIPTOR)
            data.write()
            if (!remote.transact(code, data, reply, 0)) {
                throw RemoteException("ANW transaction $code rejected")
            }
            reply.readException()
            val result = reply.readInt()
            readTail(reply)
            return result
        } finally {
            reply.recycle()
            data.recycle()
        }
    }

    private fun transactString(remote: IBinder, code: Int, write: Parcel.() -> Unit): String? {
        val data = Parcel.obtain()
        val reply = Parcel.obtain()
        try {
            data.writeInterfaceToken(SERVICE_DESCRIPTOR)
            data.write()
            if (!remote.transact(code, data, reply, 0)) {
                throw RemoteException("ANW transaction $code rejected")
            }
            reply.readException()
            return reply.readString()
        } finally {
            reply.recycle()
            data.recycle()
        }
    }

    private fun report(message: String) {
        Log.i(TAG, message)
        runCatching { diagnosticSink(message) }
    }
}
