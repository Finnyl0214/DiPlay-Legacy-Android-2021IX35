package com.shilapi.xcertplay

import com.shilapi.xcertplay.host.R
import com.shilapi.xcertplay.bluetooth.CarPlayBluetoothManager
import com.shilapi.xcertplay.iap2.session.Iap2Session
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import com.shilapi.xcertplay.usbbt.UsbBtSelfTest
import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothSocket
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Bundle
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Spinner
import android.widget.ArrayAdapter
import android.widget.CheckBox
import androidx.activity.ComponentActivity
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.content.ContextCompat
import java.io.File
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** A bounded transport probe: never sends iAP2 payloads or changes pairing. */
@SuppressLint("MissingPermission")
class BluetoothDiagnosticActivity : ComponentActivity() {
    private val service = UUID.fromString("00000000-deca-fade-deca-deafdecacafe")
    private val worker = Executors.newSingleThreadExecutor()
    private lateinit var output: TextView
    private lateinit var start: Button
    private lateinit var modeSelector: Spinner
    private lateinit var adbApproval: CheckBox
    private val report = StringBuilder()
    @Volatile private var probePid = 0
    @Volatile private var cancelled = false
    private var device: BluetoothDevice? = null
    private var registered = false
    private var busy = false
    @Volatile private var activeAnwProbe: AutoCloseable? = null
    private val sdpReceived = java.util.concurrent.atomic.AtomicBoolean(false)
    private val knownNames = java.util.concurrent.CopyOnWriteArrayList<String>()
    private val permissions = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { result ->
        if (result.values.all { it }) begin() else record("Required Bluetooth permissions denied")
    }
    private val receiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) {
            @Suppress("DEPRECATION")
            val peer = intent.getParcelableExtra<BluetoothDevice>(BluetoothDevice.EXTRA_DEVICE) ?: return
            if (peer.address != device?.address) return
            when (intent.action) {
                BluetoothDevice.ACTION_UUID -> {
                    sdpReceived.set(true)
                    @Suppress("DEPRECATION")
                    val values = intent.getParcelableArrayExtra(BluetoothDevice.EXTRA_UUID)
                        ?.filterIsInstance<android.os.ParcelUuid>()?.map { it.uuid }
                    record("SDP result received: count=${values?.size} iAP2Present=${values?.contains(service)}")
                    values?.forEach { record("SDP service category=${category(it)}") }
                }
                BluetoothDevice.ACTION_ACL_CONNECTED -> record("Selected peer ACL connected")
                BluetoothDevice.ACTION_ACL_DISCONNECTED -> record("Selected peer ACL disconnected")
                BluetoothDevice.ACTION_BOND_STATE_CHANGED -> record("Selected peer bond state=${peer.bondState}")
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val layout = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(24, 24, 24, 24) }
        val hybrid = CarPlayBluetoothManager.get(this)
        val usingUsb = hybrid.usbDongle?.isSupported() == true
        val usingAnw = !usingUsb && hybrid.ix35Anw?.isSupported() == true
        layout.addView(TextView(this).apply {
            text = when {
                usingUsb -> getString(R.string.settings_usb_bluetooth_diagnostic_description)
                usingAnw -> "ix35 原厂 ANW 蓝牙诊断\n可单独执行只读状态检查，或执行 iAP2 链路层握手测试。握手测试会发送标准 iAP2 标记与同步数据，但不发送 MFi 认证数据、不启动 CarPlay 控制会话。无需 MFi 身份文件或 Wi-Fi 热点。"
                else -> "完整蓝牙诊断\n一次采集权限、蓝牙状态、服务发现、两种连接及前后系统日志。每种连接最多 45 秒，只打开通道，不发送数据；超时后自动释放独立诊断进程。无需热点。系统日志需要已授权的本机 ADB；不可用时会明确注明。完成后在本页保存完整报告。"
            }
        })
        modeSelector = Spinner(this).apply {
            adapter = ArrayAdapter(this@BluetoothDiagnosticActivity, android.R.layout.simple_spinner_dropdown_item,
                listOf("两种连接（安全优先）", "仅安全连接", "仅非安全连接（不发送数据）"))
        }
        modeSelector.visibility = if (usingUsb || usingAnw) android.view.View.GONE else android.view.View.VISIBLE
        layout.addView(modeSelector)
        adbApproval = CheckBox(this).apply { text = "尝试请求本机 ADB 授权（需开启网络 ADB 5555；可跳过）" }
        adbApproval.visibility = if (usingUsb || usingAnw) android.view.View.GONE else android.view.View.VISIBLE
        layout.addView(adbApproval)
        start = Button(this).apply {
            text = when {
                usingUsb -> getString(R.string.settings_usb_bluetooth_self_test)
                usingAnw -> "执行 ANW 只读诊断"
                else -> "一键完整诊断（约 2–4 分钟）"
            }
            setOnClickListener { requestProbe() }
        }
        layout.addView(start)
        if (usingAnw) layout.addView(Button(this).apply {
            text = "执行 ANW + iAP2 链路握手测试"
            setOnClickListener { beginAnwIap2LinkDiagnostic() }
        })
        layout.addView(Button(this).apply { text = "停止诊断"; setOnClickListener { cancel() } })
        layout.addView(Button(this).apply { text = "保存完整诊断报告"; setOnClickListener { saveReport() } })
        output = TextView(this).apply { setTextIsSelectable(true) }
        layout.addView(ScrollView(this).apply { addView(output) }, LinearLayout.LayoutParams(-1, 0, 1f))
        setContentView(layout)
        val file = File(filesDir, "logs/bluetooth-diagnostic.log")
        if (file.isFile) output.text = file.readText()
        val filter = IntentFilter().apply {
            addAction(BluetoothDevice.ACTION_UUID)
            addAction(BluetoothDevice.ACTION_ACL_CONNECTED)
            addAction(BluetoothDevice.ACTION_ACL_DISCONNECTED)
            addAction(BluetoothDevice.ACTION_BOND_STATE_CHANGED)
        }
        ContextCompat.registerReceiver(this, receiver, filter, ContextCompat.RECEIVER_EXPORTED)
        registered = true
    }

    private fun requestProbe() {
        if (busy) return
        if (CarPlayBackgroundSession.hasSession()) {
            record("请先在 DiPlay 主页面断开当前会话，避免两个连接互相干扰。")
            return
        }
        val hybrid = CarPlayBluetoothManager.get(this)
        if (hybrid.ix35Anw?.isSupported() == true) {
            beginAnwReadOnlyDiagnostic()
            return
        }
        if (hybrid.usbDongle?.isSupported() == true) {
            beginUsbDiagnostic()
            return
        }
        if (Build.VERSION.SDK_INT >= 31) permissions.launch(arrayOf(Manifest.permission.BLUETOOTH_CONNECT, Manifest.permission.BLUETOOTH_SCAN))
        else begin()
    }

    /** Queries only the factory ANW Binder read APIs; never opens or closes an iAP2 socket. */
    private fun beginAnwReadOnlyDiagnostic() {
        if (busy) return
        busy = true
        cancelled = false
        start.isEnabled = false
        worker.execute {
            try {
                val adapter = CarPlayBluetoothManager.get(applicationContext).ix35Anw
                    ?: error("ix35 ANW adapter is unavailable")
                record("ix35 ANW read-only diagnostic; Android=${Build.VERSION.RELEASE}; model=${Build.MODEL}")
                record("ANW service package present=${adapter.isSupported()}")
                val radio = adapter.readRadioPowerState()
                record("ANW Bluetooth radio state=${when (radio) { true -> "ON"; false -> "OFF"; null -> "unavailable" }}")
                val phone = adapter.getConnectedDevice()
                val addressValid = phone?.address?.matches(Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")) == true
                val nameAvailable = phone?.name?.let { it.isNotBlank() && it != "iPhone" } == true
                record("ANW HFP connected phone=${phone != null}; addressValid=$addressValid; nameAvailable=$nameAvailable")
                record("ANW socket/iAP2 transport=NOT TESTED; no socket init/connect/disconnect/write was performed")
                record("Read-only diagnostic complete. This verifies Binder read paths only, not iAP2 or CarPlay authentication.")
            } catch (error: Exception) {
                record("ANW read-only diagnostic error ${error.javaClass.simpleName}: ${error.message}")
            } finally {
                busy = false
                runOnUiThread { if (!isFinishing) start.isEnabled = true }
            }
        }
    }

    /**
     * Sends only the standard iAP2 link-layer marker/synchronization exchange over the factory
     * ANW socket. It deliberately stops before accessory identification, MFi authentication,
     * CSM control messages, Wi-Fi handoff, or CarPlay startup.
     */
    private fun beginAnwIap2LinkDiagnostic() {
        if (busy) return
        if (CarPlayBackgroundSession.hasSession()) {
            record("请先在 DiPlay 主页面断开当前会话，避免两个连接互相干扰。")
            return
        }
        busy = true
        cancelled = false
        start.isEnabled = false
        worker.execute {
            var adapter: com.shilapi.xcertplay.bluetooth.Ix35AnwBluetoothAdapter? = null
            var stream: BlockingDuplexByteStream? = null
            var session: Iap2Session? = null
            try {
                val anwAdapter = CarPlayBluetoothManager.get(applicationContext).ix35Anw
                    ?: error("ix35 ANW adapter is unavailable")
                adapter = anwAdapter
                val phone = anwAdapter.getConnectedDevice()
                    ?: error("No connected HFP phone was reported by the ANW service")
                check(phone.address.matches(Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$"))) {
                    "ANW returned an invalid remote Bluetooth address"
                }
                anwAdapter.setDiagnosticSink { message -> record(message) }
                record("ANW iAP2 link-layer diagnostic started; protocolWrites=standard-marker-and-sync-only; mfi=false; carPlayBootstrap=false")
                val openedStream = anwAdapter.openRfcommStream(phone.address, service.toString())
                stream = openedStream
                activeAnwProbe = openedStream
                if (cancelled) throw InterruptedException("ANW iAP2 diagnostic cancelled before link negotiation")
                record("ANW socket opened; starting existing wireless iAP2 link engine")
                val openedSession = Iap2Session.openWireless(
                    openedStream,
                    traceContext = "ix35-anw-diagnostic",
                    onTrace = ::record,
                )
                session = openedSession
                activeAnwProbe = openedSession
                stream = null // Iap2Session owns and closes the ANW stream.
                val ready = openedSession.awaitReady(15_000L)
                record("ANW iAP2 link-layer handshake ready=$ready")
                if (!ready) {
                    record("ANW iAP2 link-layer handshake did not complete within 15000ms; no accessory identification or MFi exchange was attempted")
                }
            } catch (error: Throwable) {
                record("ANW iAP2 link diagnostic error=${error.javaClass.simpleName}: ${error.message}")
            } finally {
                runCatching { session?.close() }
                    .onFailure { record("ANW iAP2 session close error=${it.javaClass.simpleName}: ${it.message}") }
                runCatching { stream?.close() }
                    .onFailure { record("ANW stream close error=${it.javaClass.simpleName}: ${it.message}") }
                activeAnwProbe = null
                adapter?.setDiagnosticSink {}
                busy = false
                runOnUiThread { if (!isFinishing) start.isEnabled = true }
            }
        }
    }

    private fun beginUsbDiagnostic() {
        busy = true
        cancelled = false
        start.isEnabled = false
        val app = applicationContext
        worker.execute {
            val dongle = CarPlayBluetoothManager.get(app).usbDongle
            val restoreService = dongle?.isEnabled() == true || AirPlayPersistence.loadWirelessEnabled(app)
            try {
                record("USB Bluetooth diagnostic; native Android SDP/socket probes are not used for this adapter")
                dongle?.disable()
                val result = UsbBtSelfTest.run(app)
                record("USB hardware test: ${result.status}; ${result.message}")
                result.details.forEach { record(it) }
                val detectedDevice = result.device
                if (result.needsPermission && detectedDevice != null) {
                    val manager = app.getSystemService(Context.USB_SERVICE) as? android.hardware.usb.UsbManager
                    if (manager != null) runOnUiThread {
                        com.shilapi.xcertplay.usbbt.UsbBtDongleDetector(app).ensurePermission(manager,
                            com.shilapi.xcertplay.usbbt.UsbBtDongleDetector.Dongle(detectedDevice, result.vendorLabel.orEmpty(), false))
                    }
                }
                record("HCI hardware success does not verify iPhone SDP, RFCOMM or CarPlay authentication")
            } catch (error: Exception) {
                record("USB diagnostic error: ${error.message}")
            } finally {
                Thread.interrupted() // allow restoration even if this diagnostic was cancelled
                if (restoreService) {
                    val restored = runCatching { dongle?.enable() == true }.getOrDefault(false)
                    record("USB Bluetooth service restored=$restored")
                }
                busy = false
                runOnUiThread { if (!isFinishing) start.isEnabled = true }
            }
        }
    }

    /** Context.getSystemService(Class) 需要 API 23；4.4 线走字符串常量。 */
    private fun bluetoothAdapterCompat(): android.bluetooth.BluetoothAdapter? =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            getSystemService(BluetoothManager::class.java)?.adapter
        } else {
            @Suppress("DEPRECATION")
            (getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter
        }

    private fun begin() {
        if (busy) return
        synchronized(report) { report.clear() }
        cancelled = false
        sdpReceived.set(false)
        busy = true
        start.isEnabled = false
        val allowAdbApproval = adbApproval.isChecked
        val modes = when (modeSelector.selectedItemPosition) {
            1 -> listOf(true)
            2 -> listOf(false)
            else -> listOf(true, false)
        }
        worker.execute {
            val since = java.text.SimpleDateFormat("MM-dd HH:mm:ss.SSS", java.util.Locale.ROOT).format(java.util.Date())
            val collector = BluetoothSystemCollector(applicationContext, ::record)
            try {
                record("Bluetooth transport diagnostic v3; Android=${Build.VERSION.RELEASE}; model=${Build.MODEL}")
                record("Board=${Build.BOARD}; hardware=${Build.HARDWARE}; build=${Build.DISPLAY}")
                record("Permissions connect=${androidx.core.content.ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT)} scan=${androidx.core.content.ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_SCAN)}; pre-Android12 uses legacy permissions")
                val adapter = bluetoothAdapterCompat() ?: error("Android Bluetooth adapter absent")
                record("Adapter enabled=${adapter.isEnabled}; state=${adapter.state}; discovering=${adapter.isDiscovering}")
                check(adapter.isEnabled) { "Enable Bluetooth first" }
                val address = DiPlayPreferences.phoneAddress(this)
                device = adapter.bondedDevices.firstOrNull { it.address.equals(address, true) }
                val peer = device ?: error("Selected phone is not in Android bonded list; select it in connection setup")
                knownNames.clear()
                adapter.bondedDevices.mapNotNull { it.name }.filter { it.isNotBlank() }.forEach { knownNames.add(it) }
                adapter.name?.takeIf { it.isNotBlank() }?.let { knownNames.add(it) }
                record("Profile states A2DP=${adapter.getProfileConnectionState(android.bluetooth.BluetoothProfile.A2DP)} headset=${adapter.getProfileConnectionState(android.bluetooth.BluetoothProfile.HEADSET)}; states are adapter-wide, not proof for selected peer")
                collector.prepare(allowAdbApproval)
                collector.snapshot("before connection tests", since)
                if (cancelled) return@execute
                record("Selected peer bond=${peer.bondState}; type=${peer.type}; class=${peer.bluetoothClass?.deviceClass}; bondedCount=${adapter.bondedDevices.size}")
                val cached = peer.uuids?.map { it.uuid }
                record("Cached SDP count=${cached?.size}; iAP2Present=${cached?.contains(service)} (cache is not proof of current availability)")
                record("Discovery cancellation=${adapter.cancelDiscovery()}")
                record("SDP request accepted=${peer.fetchUuidsWithSdp()}")
                // Allow the asynchronous SDP broadcast to arrive before socket probing.
                repeat(12) { if (cancelled) return@execute; Thread.sleep(1000) }
                record("SDP response within 12 seconds=${sdpReceived.get()}")
                for (secure in modes) {
                    if (cancelled) break
                    probe(peer, secure)
                    Thread.sleep(1000)
                }
                record("Diagnostic complete; missing SDP response is inconclusive; socket success does not verify iAP2 authentication")
            } catch (error: Exception) {
                record("Diagnostic error ${error.javaClass.simpleName}: ${error.message}")
            } finally {
                collector.snapshot("after connection tests", since)
                collector.close()
                record("完整诊断结束。系统日志若显示不可用，本报告不能确定底层拒绝原因。请保存报告。")
                runOnUiThread { busy = false; start.isEnabled = true }
            }
        }
    }

    private fun probe(peer: BluetoothDevice, secure: Boolean): Boolean {
        val mode = if (secure) "secure" else "insecure"
        val stage = java.util.concurrent.atomic.AtomicReference("process-start")
        val outcome = java.util.concurrent.atomic.AtomicReference("no result")
        val finished = java.util.concurrent.CountDownLatch(1)
        val active = java.util.concurrent.atomic.AtomicBoolean(true)
        val pid = java.util.concurrent.atomic.AtomicInteger(0)
        val receiver = object : android.os.ResultReceiver(android.os.Handler(mainLooper)) {
            override fun onReceiveResult(code: Int, data: Bundle?) {
                val childPid = data?.getInt("pid") ?: return
                if (!active.get()) {
                    if (childPid > 0 && childPid != android.os.Process.myPid()) android.os.Process.killProcess(childPid)
                    return
                }
                pid.set(childPid)
                probePid = childPid
                val text = data.getString("text") ?: "unknown"
                when (code) {
                    1 -> { stage.set(text); record("RFCOMM $mode stage=$text") }
                    2 -> { outcome.set(text); finished.countDown() }
                    3 -> outcome.set(text)
                }
            }
        }
        val began = android.os.SystemClock.elapsedRealtime()
        record("RFCOMM $mode starting in disposable app process; deadline=45000ms")
        runOnUiThread {
            runCatching { startService(Intent(this, BluetoothProbeService::class.java)
                .putExtra("receiver", receiver).putExtra("address", peer.address).putExtra("secure", secure)) }
                .onFailure { outcome.set("Process start failed: ${it.javaClass.simpleName}"); finished.countDown() }
        }
        var nextProgress = 5000L
        try {
            while (!finished.await(250, TimeUnit.MILLISECONDS)) {
                val elapsed = android.os.SystemClock.elapsedRealtime() - began
                if (cancelled || elapsed >= 45000) {
                    record("RFCOMM $mode ${if (cancelled) "cancelled" else "timed out"}; blockedStage=${stage.get()}; elapsedMs=$elapsed; lastResult=${outcome.get()}")
                    return false
                }
                if (elapsed >= nextProgress) {
                    record("RFCOMM $mode waiting; stage=${stage.get()}; elapsedMs=$elapsed")
                    nextProgress += 5000
                }
            }
            record("RFCOMM $mode result=${outcome.get()}; elapsedMs=${android.os.SystemClock.elapsedRealtime() - began}")
            return true
        } finally {
            active.set(false)
            val child = pid.get()
            if (child > 0 && child != android.os.Process.myPid()) android.os.Process.killProcess(child)
            probePid = 0
            record("RFCOMM $mode diagnostic process released")
        }
    }
    private fun category(uuid: UUID): String = when (uuid) {
        service -> "iAP2"
        UUID.fromString("0000110a-0000-1000-8000-00805f9b34fb") -> "A2DP source"
        UUID.fromString("0000110b-0000-1000-8000-00805f9b34fb") -> "A2DP sink"
        UUID.fromString("0000111e-0000-1000-8000-00805f9b34fb") -> "Hands-free"
        else -> "other"
    }

    private fun record(message: String) {
        var filtered = message.replace('\n', ' ').replace('\r', ' ')
        knownNames.forEach { filtered = filtered.replace(it, "[device]", ignoreCase = true) }
        val safe = DiagnosticRedactor.redact(filtered) ?: return
        synchronized(report) {
            report.append(java.text.SimpleDateFormat("HH:mm:ss.SSS", java.util.Locale.ROOT).format(java.util.Date())).append("  ").append(safe).append('\n')
            val text = report.toString()
            val file = File(filesDir, "logs/bluetooth-diagnostic.log")
            file.parentFile?.mkdirs()
            file.writeText(text)
            val diplayLog = File(filesDir, "logs/diplay.log")
            if (diplayLog.exists() && diplayLog.length() <= 512 * 1024L) {
                runCatching {
                    val line = java.text.SimpleDateFormat("HH:mm:ss.SSS", java.util.Locale.ROOT).format(java.util.Date()) + "  [BT-DIAG] " + safe + "\n"
                    diplayLog.appendText(line)
                }
            }
            runOnUiThread { output.text = text }
        }
    }

    private fun saveReport() {
        if (busy) { record("诊断仍在运行，请结束后保存，避免缺少后半段结果。"); return }
        worker.execute {
            runCatching {
                val text = buildString {
                    appendLine("DiPlay complete Bluetooth diagnostic; Android ${Build.VERSION.RELEASE}; ${Build.MODEL}")
                    val names = listOf("bluetooth-diagnostic.log") + SessionLogFile.REPORT_NAMES.toList()
                    names.forEach { name ->
                        val file = File(filesDir, "logs/$name")
                        if (file.isFile) {
                            appendLine("--- $name ---")
                            file.useLines { lines -> lines.forEach { line -> DiagnosticRedactor.redact(line)?.let { appendLine(it) } } }
                        }
                    }
                }
                if (Build.VERSION.SDK_INT >= 29) {
                    val name = "DiPlay-Bluetooth-${java.text.SimpleDateFormat("yyyyMMdd-HHmmss", java.util.Locale.ROOT).format(java.util.Date())}.txt"
                    DiagnosticExportStore.saveToDownloads(contentResolver, name, text)
                    record("完整报告已保存到 Downloads/DiPlay/$name")
                } else record("请返回设置使用保存诊断报告选择文件位置。")
            }.onFailure { record("报告保存失败：${it.javaClass.simpleName}: ${it.message}") }
        }
    }

    private fun cancel() {
        cancelled = true
        activeAnwProbe?.let { probe -> Thread({ runCatching { probe.close() } }, "ix35-anw-probe-cancel").start() }
        if (probePid > 0 && probePid != android.os.Process.myPid()) android.os.Process.killProcess(probePid)
        record("Diagnostic cancellation requested")
    }

    override fun onDestroy() {
        cancelled = true
        activeAnwProbe?.let { probe -> runCatching { probe.close() } }
        if (probePid > 0 && probePid != android.os.Process.myPid()) android.os.Process.killProcess(probePid)
        worker.shutdownNow()
        if (registered) unregisterReceiver(receiver)
        super.onDestroy()
    }

}
