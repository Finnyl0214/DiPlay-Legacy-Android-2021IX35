package com.shilapi.xcertplay.bluetooth

import android.content.Context
import android.util.Log
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import com.shilapi.xcertplay.usbbt.UsbBtBluetoothAdapter

class HybridBluetoothAdapter(
    val hsae: HsaeBluetoothAdapter,
    val android: AndroidBluetoothAdapter,
    /** USB dongle stack; preferred for data channels when plugged in. */
    val usbDongle: UsbBtBluetoothAdapter? = null,
    /** ix35 factory ANW data socket; preferred only when its service package exists. */
    val ix35Anw: Ix35AnwBluetoothAdapter? = null,
) : CarPlayBluetooth {

    companion object {
        private const val TAG = "HybridBluetoothAdapter"
    }

    override fun isSupported(): Boolean =
        ix35Anw?.isSupported() == true || hsae.isSupported() || android.isSupported() || usbDongle?.isSupported() == true

    override fun isEnabled(): Boolean {
        if (ix35Anw?.isSupported() == true) return ix35Anw.isEnabled()
        if (usbDongle?.isSupported() == true) return usbDongle.isEnabled()
        if (hsae.isSupported() && hsae.isEnabled()) return true
        return android.isEnabled()
    }

    override fun enable(): Boolean {
        if (ix35Anw?.isSupported() == true) return ix35Anw.enable()
        var ok = false
        if (hsae.isSupported()) {
            ok = hsae.enable()
        }
        if (android.enable()) ok = true
        // USB dongle: enable whenever one is plugged in. Previously this only
        // ran when neither HSAE nor Android existed, so on T3 (HSAE present
        // but audio-only) the dongle was never enabled and wireless could
        // never reach it (P0-1). Plugging in a dongle is itself the opt-in.
        if (usbDongle?.isSupported() == true) {
            return usbDongle.enable()
        }
        return ok
    }

    override fun disable() {
        usbDongle?.disable()
    }

    override fun getAdapterName(): String {
        if (ix35Anw?.isSupported() == true) return ix35Anw.getAdapterName()
        val parts = mutableListOf<String>()
        if (hsae.isSupported()) parts.add("HSAE")
        if (android.isSupported()) parts.add("Android")
        if (usbDongle?.isSupported() == true) parts.add("USB")
        return if (parts.isEmpty()) android.getAdapterName()
        else "Hybrid(${parts.joinToString("+")})"
    }

    override fun getPairedDevices(): List<CarPlayBluetoothDevice> {
        // ix35's ANW service currently exposes the connected HFP phone; do not
        // mix in addresses from a different Bluetooth controller/backend.
        if (ix35Anw?.isSupported() == true) {
            return runCatching { ix35Anw.getPairedDevices() }.getOrDefault(emptyList())
        }
        // Merge all non-ix35 stacks, dedupe by MAC.
        val seen = HashSet<String>()
        val merged = ArrayList<CarPlayBluetoothDevice>()
        fun addAll(devices: List<CarPlayBluetoothDevice>) {
            for (device in devices) {
                val key = device.address.lowercase()
                if (seen.add(key)) merged.add(device)
            }
        }
        if (ix35Anw?.isSupported() == true) {
            runCatching { addAll(ix35Anw.getPairedDevices()) }
        }
        if (hsae.isSupported()) runCatching { addAll(hsae.getPairedDevices()) }
        runCatching { addAll(android.getPairedDevices()) }
        usbDongle?.let { runCatching { addAll(it.getPairedDevices()) } }
        return merged
    }

    override fun getConnectedDevice(): CarPlayBluetoothDevice? {
        // ix35's ANW HFP query returns the remote phone address used by its
        // private iAP2 socket; Android BluetoothAdapter is unavailable there.
        if (ix35Anw?.isSupported() == true) {
            return runCatching { ix35Anw.getConnectedDevice() }.getOrNull()
        }
        // Dongle first: it is the data-capable stack. HSAE may report a
        // connected audio-only device that cannot carry iAP2.
        usbDongle?.let { runCatching { it.getConnectedDevice() } }?.getOrNull()?.let { return it }
        android.getConnectedDevice()?.let { return it }
        if (hsae.isSupported()) {
            runCatching { hsae.getConnectedDevice() }.getOrNull()?.let { return it }
        }
        return null
    }

    override fun localAddress(context: Context): String? {
        // Dongle first: its BD_ADDR is the one the iPhone must pair with for
        // the data channel. Falling back to HSAE's address would pair the
        // iPhone to the audio-only module.
        usbDongle?.let { runCatching { it.localAddress(context) } }?.getOrNull()
            ?.takeIf { !it.isNullOrBlank() }?.let { return it }
        if (hsae.isSupported()) {
            val hsaeAddr = hsae.localAddress(context)
            if (!hsaeAddr.isNullOrBlank()) return hsaeAddr
        }
        android.localAddress(context)?.let { return it }
        return null
    }

    override fun openRfcommStream(deviceAddress: String, uuid: String): BlockingDuplexByteStream {
        if (ix35Anw?.isSupported() == true) return ix35Anw.openRfcommStream(deviceAddress, uuid)
        val dongle = usbDongle
        if (dongle?.isSupported() == true) {
            if (!dongle.isEnabled()) throw java.io.IOException("USB Bluetooth not enabled")
            // An iAP2 advertisement and bond belong to one local adapter address.
            // Switching to GOC/HSAE cannot resume the dongle's connection.
            return dongle.openRfcommStream(deviceAddress, uuid)
        }
        var failure: Exception? = null
        if (android.isSupported() && android.isEnabled()) {
            try {
                return android.openRfcommStream(deviceAddress, uuid)
            } catch (interrupted: InterruptedException) {
                Thread.currentThread().interrupt()
                throw interrupted
            } catch (error: Exception) {
                failure = error
            }
        }
        if (hsae.isSupported()) return hsae.openRfcommStream(deviceAddress, uuid)
        throw java.io.IOException("Bluetooth RFCOMM unavailable: ${failure?.message ?: "no Bluetooth stack available"}", failure)
    }
}
