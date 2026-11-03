package com.shilapi.xcertplay.bluetooth.ix35

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.Binder
import android.os.IBinder
import android.os.IInterface
import android.os.Parcel
import android.os.RemoteException
import android.util.Log
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import java.io.IOException
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * ix35-only transport for the factory ANW Bluetooth service.
 * Transaction codes/descriptors are from the ix35 read-only v2.7 Binder audit.
 */
class Ix35AnwSocketStream private constructor(
    private val context: Context,
    private val remote: IBinder,
    private val connection: ServiceConnection,
    private val socketIndex: Int,
    private val callback: SocketCallback,
    private val onDiagnostic: (String) -> Unit,
) : BlockingDuplexByteStream {
    private val lock = Object()
    private val writeLock = Object()
    private val incoming = ArrayDeque<ByteArray>()
    private var incomingBytes = 0
    private var closed = false
    private var peerClosed = false
    private var failure: IOException? = null
    private var firstReceiveLogged = false
    private var firstWriteLogged = false
    private var firstReadLogged = false
    private var callbackBytes = 0L
    private var readCalls = 0L
    private var readBytes = 0L
    private var writeCalls = 0L
    private var writeBytes = 0L

    private fun report(message: String) {
        runCatching { onDiagnostic("ANW-TRANSPORT $message") }
        Log.i(TAG, message)
    }

    private fun enqueue(bytes: ByteArray, length: Int) {
        if (length <= 0 || length > bytes.size) return
        synchronized(lock) {
            if (closed || peerClosed) return
            if (incomingBytes + length > MAX_PENDING_BYTES) {
                failure = IOException("ANW receive queue exceeded $MAX_PENDING_BYTES bytes")
                peerClosed = true
            } else {
                callbackBytes += length
                if (!firstReceiveLogged) {
                    firstReceiveLogged = true
                    report("event=first-callback-data bytes=$length queuedBytes=$incomingBytes")
                }
                incoming.addLast(if (length == bytes.size) bytes.copyOf() else bytes.copyOf(length))
                incomingBytes += length
            }
            lock.notifyAll()
        }
    }

    override fun send(data: ByteArray) {
        if (data.isEmpty()) return
        synchronized(writeLock) {
            synchronized(lock) {
                failure?.let { throw it }
                if (closed) throw IOException("ix35 ANW Socket is closed")
            }
            var offset = 0
            while (offset < data.size) {
                val count = minOf(MAX_WRITE_CHUNK, data.size - offset)
                val chunk = if (offset == 0 && count == data.size) data else data.copyOfRange(offset, offset + count)
                val written = intArrayOf(0)
                val result = try {
                    transactInt(TRANSACTION_SOCKET_WRITE, {
                        writeInt(socketIndex)
                        writeByteArray(chunk)
                        writeInt(chunk.size)
                        writeInt(written.size)
                    }, { reply -> reply.readIntArray(written) })
                } catch (e: Exception) {
                    val io = IOException("ANWBT_SocketWrite transaction failed", e)
                    synchronized(lock) { failure = io }
                    throw io
                }
                if (result < 0) {
                    val io = IOException("ANWBT_SocketWrite returned $result")
                    synchronized(lock) { failure = io }
                    throw io
                }
                // Match the upstream HSAE contract exactly: every non-positive
                // nWritten value means the vendor accepted the complete chunk.
                val actual = if (written[0] > 0) written[0] else count
                if (actual > count) {
                    val io = IOException("ANWBT_SocketWrite returned invalid written=$actual, chunk=$count")
                    synchronized(lock) { failure = io }
                    throw io
                }
                writeCalls++
                writeBytes += actual
                if (!firstWriteLogged) {
                    firstWriteLogged = true
                    report("event=first-write-accepted ret=$result requested=$count reportedWritten=${written[0]} accepted=$actual")
                } else if (writeCalls % 64L == 0L) {
                    report("event=write-progress calls=$writeCalls bytes=$writeBytes")
                }
                offset += actual
            }
        }
    }

    override fun recv(maxBytes: Int, timeoutMillis: Long): ByteArray? {
        require(maxBytes > 0)
        require(timeoutMillis >= 0)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        synchronized(lock) {
            while (true) {
                val first = incoming.peekFirst()
                if (first != null) {
                    val part: ByteArray
                    if (first.size <= maxBytes) {
                        incoming.removeFirst()
                        incomingBytes -= first.size
                        part = first
                    } else {
                        part = first.copyOf(maxBytes)
                        incoming.removeFirst()
                        incoming.addFirst(first.copyOfRange(maxBytes, first.size))
                        incomingBytes -= maxBytes
                    }
                    readCalls++
                    readBytes += part.size
                    if (!firstReadLogged) {
                        firstReadLogged = true
                        report("event=first-read bytes=${part.size} callbackBytes=$callbackBytes")
                    } else if (readCalls % 64L == 0L) {
                        report("event=read-progress calls=$readCalls bytes=$readBytes")
                    }
                    return part
                }
                failure?.let { throw it }
                if (closed || peerClosed) return EMPTY
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return null
                try {
                    lock.wait(remaining / 1_000_000L, (remaining % 1_000_000L).toInt())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw IOException("Interrupted while waiting for ANW data", e)
                }
            }
        }
    }

    override fun close() {
        synchronized(lock) {
            if (closed) return
            closed = true
            lock.notifyAll()
        }
        // Stop delivering data immediately, but keep the callback registered until
        // Disconnect has completed. This matches the order used by the read-only
        // device audit and prevents teardown callbacks from being queued again.
        callback.detach()
        report("event=close-start socketIndex=$socketIndex callbackBytes=$callbackBytes readCalls=$readCalls readBytes=$readBytes writeCalls=$writeCalls writeBytes=$writeBytes")
        try {
            val result = transactInt(TRANSACTION_SOCKET_DISCONNECT, write = { writeInt(socketIndex) })
            report("event=socket-disconnect result=$result")
        } catch (e: Exception) {
            report("event=socket-disconnect-failed error=${e.javaClass.simpleName}")
        }
        try {
            transactVoid(TRANSACTION_UNREGISTER_SOCKET_CALLBACK) { writeStrongBinder(callback) }
            report("event=callback-unregistered")
        } catch (e: Exception) {
            report("event=callback-unregister-failed error=${e.javaClass.simpleName}")
        }
        // SocketInit allocates service-side state; release it after disconnect
        // on the normal close path as well as on connection failures.
        try {
            val result = transactInt(TRANSACTION_SOCKET_DENIT)
            report("event=socket-deinit result=$result")
        } catch (e: Exception) {
            report("event=socket-deinit-failed error=${e.javaClass.simpleName}")
        }
        try {
            context.unbindService(connection)
        } catch (e: Exception) {
            report("event=service-unbind-failed error=${e.javaClass.simpleName}")
        }
        report("event=close-complete socketIndex=$socketIndex callbackBytes=$callbackBytes readCalls=$readCalls readBytes=$readBytes writeCalls=$writeCalls writeBytes=$writeBytes")
    }

    private fun transactVoid(code: Int, write: Parcel.() -> Unit = {}) {
        val data = Parcel.obtain()
        val reply = Parcel.obtain()
        try {
            data.writeInterfaceToken(SERVICE_DESCRIPTOR)
            data.write()
            if (!remote.transact(code, data, reply, 0)) throw RemoteException("Transaction $code rejected")
            reply.readException()
        } finally {
            reply.recycle()
            data.recycle()
        }
    }

    private fun transactInt(
        code: Int,
        write: Parcel.() -> Unit = {},
        readTail: (Parcel) -> Unit = {}
    ): Int {
        val data = Parcel.obtain()
        val reply = Parcel.obtain()
        try {
            data.writeInterfaceToken(SERVICE_DESCRIPTOR)
            data.write()
            if (!remote.transact(code, data, reply, 0)) throw RemoteException("Transaction $code rejected")
            reply.readException()
            val result = reply.readInt()
            readTail(reply)
            return result
        } finally {
            reply.recycle()
            data.recycle()
        }
    }

    private class SocketCallback(private val onDiagnostic: (String) -> Unit) : Binder(), IInterface {
        private var callbackCount = 0L
        private var callbackBytes = 0L
        private var firstCallbackLogged = false
        private var wrongSocketCallbacks = 0L

        private fun report(message: String) {
            runCatching { onDiagnostic("ANW-TRANSPORT $message") }
            Log.i(TAG, message)
        }
        private val callbackLock = Any()
        private val pending = ArrayDeque<Pair<Int, ByteArray>>()
        private var pendingBytes = 0
        private var expectedIndex = -1
        private var receiver: ((ByteArray, Int) -> Unit)? = null
        private var detached = false

        init { attachInterface(this, CALLBACK_DESCRIPTOR) }
        override fun asBinder(): IBinder = this

        fun attach(index: Int, target: (ByteArray, Int) -> Unit) {
            val buffered = ArrayList<Pair<Int, ByteArray>>()
            synchronized(callbackLock) {
                if (detached) return
                expectedIndex = index
                receiver = target
                while (!pending.isEmpty()) buffered.add(pending.removeFirst())
                pendingBytes = 0
            }
            buffered.forEach { (socket, bytes) ->
                if (socket == index) target(bytes, bytes.size)
            }
        }

        fun detach() {
            synchronized(callbackLock) {
                detached = true
                receiver = null
                pending.clear()
                pendingBytes = 0
            }
        }

        private fun dispatch(index: Int, bytes: ByteArray, length: Int) {
            if (length <= 0 || length > bytes.size) {
                report("event=callback-invalid-length length=$length arraySize=${bytes.size}")
                return
            }
            val target: ((ByteArray, Int) -> Unit)?
            synchronized(callbackLock) {
                if (detached) return
                callbackCount++
                callbackBytes += length
                if (!firstCallbackLogged) {
                    firstCallbackLogged = true
                    report("event=first-callback socketIndex=$index bytes=$length")
                } else if (callbackCount % 64L == 0L) {
                    report("event=callback-progress count=$callbackCount bytes=$callbackBytes")
                }
                target = receiver
                if (target == null) {
                    if (pendingBytes + length <= MAX_PENDING_BYTES) {
                        val copy = if (length == bytes.size) bytes.copyOf() else bytes.copyOf(length)
                        pending.addLast(Pair(index, copy))
                        pendingBytes += copy.size
                    } else {
                        Log.e(TAG, "Dropping early ANW callback data: pending buffer limit exceeded")
                    }
                    return
                }
                if (expectedIndex != index) {
                    wrongSocketCallbacks++
                    if (wrongSocketCallbacks == 1L) report("event=callback-wrong-socket expected=$expectedIndex actual=$index")
                    return
                }
            }
            target?.invoke(bytes, length)
        }

        override fun onTransact(code: Int, data: Parcel, reply: Parcel?, flags: Int): Boolean {
            if (code == INTERFACE_TRANSACTION) {
                reply?.writeString(CALLBACK_DESCRIPTOR)
                return true
            }
            if (code != CALLBACK_TRANSACTION_DATA_IND) return super.onTransact(code, data, reply, flags)
            val tokenPosition = data.dataPosition()
            try {
                data.enforceInterface(CALLBACK_DESCRIPTOR)
            } catch (first: SecurityException) {
                data.setDataPosition(tokenPosition)
                data.enforceInterface(UPSTREAM_CALLBACK_DESCRIPTOR)
            }
            val index = data.readInt()
            val bytes = data.createByteArray()
            val length = data.readInt()
            if (bytes != null) dispatch(index, bytes, length)
            reply?.writeNoException()
            return true
        }
    }

    companion object {
        private const val TAG = "Ix35AnwSocketStream"
        private const val SERVICE_PACKAGE = "com.anwbtsdk.service"
        private const val SERVICE_CLASS = "com.anwbtsdk.service.AnwBTSdkService"
        private const val SERVICE_DESCRIPTOR = "com.anwbtsdk.service.IAnwPhoneLink"
        private const val CALLBACK_DESCRIPTOR = "com.anwbtsdk.service.IAnwSocketDataCallBack"
        private const val UPSTREAM_CALLBACK_DESCRIPTOR = "com.anwsdk.service.IAnwSocketDataCallBack"

        // ix35 v2.7 audit; upstream DiPlay's corresponding codes are one lower.
        private const val TRANSACTION_SOCKET_INIT = 223
        private const val TRANSACTION_SOCKET_DENIT = 225
        private const val TRANSACTION_SOCKET_CONNECT = 226
        private const val TRANSACTION_SOCKET_DISCONNECT = 227
        private const val TRANSACTION_SOCKET_WRITE = 228
        private const val TRANSACTION_REGISTER_SOCKET_CALLBACK = 232
        private const val TRANSACTION_UNREGISTER_SOCKET_CALLBACK = 233
        private const val TRANSACTION_GET_SOCKET_INIT_STATUS = 234
        private const val CALLBACK_TRANSACTION_DATA_IND = 1
        private const val MAX_WRITE_CHUNK = 4096
        private const val MAX_PENDING_BYTES = 1024 * 1024
        private val EMPTY = ByteArray(0)

        /** Call from a worker thread; address must be the paired iPhone MAC. */
        @Throws(IOException::class)
        fun connect(
            context: Context,
            address: String,
            uuidString: String,
            onDiagnostic: (String) -> Unit = {},
        ): BlockingDuplexByteStream {
            fun report(message: String) {
                runCatching { onDiagnostic("ANW-TRANSPORT $message") }
                Log.i(TAG, message)
            }
            val connectStartedNanos = System.nanoTime()
            var stage = "validate-address"
            report("event=connect-start")
            require(address.matches(Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$"))) { "Invalid Bluetooth address" }
            val appContext = context.applicationContext
            val latch = CountDownLatch(1)
            var binder: IBinder? = null
            val serviceConnection = object : ServiceConnection {
                override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
                    binder = service
                    report("event=service-connected binderPresent=${service != null} binderAlive=${service?.isBinderAlive == true}")
                    latch.countDown()
                }
                override fun onServiceDisconnected(name: ComponentName?) {
                    binder = null
                    report("event=service-disconnected")
                }
            }
            val intent = Intent().setComponent(ComponentName(SERVICE_PACKAGE, SERVICE_CLASS))
            val bound = try {
                appContext.bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE)
            } catch (e: Exception) {
                throw IOException("Could not bind ix35 ANW service", e)
            }
            report("event=bind-service result=$bound")
            if (!bound) throw IOException("ix35 ANW service bindService returned false")
            var callback: SocketCallback? = null
            var callbackRegistered = false
            var socketInitialized = false
            var socketConnected = false
            var connectedSocketIndex = -1
            try {
                stage = "wait-for-binder"
                if (!latch.await(5, TimeUnit.SECONDS)) throw IOException("Timed out binding ix35 ANW service")
                val remote = binder ?: throw IOException("ix35 ANW service returned a null Binder")
                if (!remote.isBinderAlive) throw IOException("ix35 ANW Binder is not alive")
                // This reports the ANW socket subsystem state, not Bluetooth RF power.
                // The AIDL defines ANWBT_GetSocketInitStatus at transaction 234.
                stage = "get-socket-init-status"
                val socketInitStatus = transact(remote, TRANSACTION_GET_SOCKET_INIT_STATUS, write = {})
                val socketSubsystemInitialized = socketInitStatus != 0
                report("event=socket-subsystem-status result=$socketInitStatus initialized=$socketSubsystemInitialized")
                val socketCallback = SocketCallback(::report)
                callback = socketCallback
                stage = "register-callback"
                transactVoid(remote, TRANSACTION_REGISTER_SOCKET_CALLBACK) { writeStrongBinder(socketCallback) }
                callbackRegistered = true
                report("event=callback-registered")
                stage = "socket-init"
                val initResult = transact(remote, TRANSACTION_SOCKET_INIT, write = {})
                report("event=socket-init result=$initResult")
                if (initResult < 0) throw IOException("ANWBT_SocketInit returned $initResult")
                socketInitialized = true
                val index = intArrayOf(-1)
                val guidBytes = uuidToGuidBytes(uuidString)
                val rfcBytes = uuidToBytes(uuidString)
                stage = "socket-connect-guid"
                var connectResult = transact(
                    remote,
                    TRANSACTION_SOCKET_CONNECT,
                    write = {
                        writeInt(1)
                        writeString(address)
                        writeByteArray(guidBytes)
                        writeInt(0)
                        writeLong(0L)
                        writeLong(0L)
                        writeInt(index.size)
                    },
                    readTail = { reply -> reply.readIntArray(index) }
                )
                report("event=socket-connect-guid result=$connectResult socketIndex=${index[0]}")
                // Keep the upstream HSAE compatibility fallback: some vendor
                // builds expect the UUID in RFC byte order rather than GUID order.
                if (connectResult < 0 || index[0] < 0) {
                    index[0] = -1
                    stage = "socket-connect-rfc"
                    connectResult = transact(
                        remote,
                        TRANSACTION_SOCKET_CONNECT,
                        write = {
                            writeInt(1)
                            writeString(address)
                            writeByteArray(rfcBytes)
                            writeInt(0)
                            writeLong(0L)
                            writeLong(0L)
                            writeInt(index.size)
                        },
                        readTail = { reply -> reply.readIntArray(index) }
                    )
                    report("event=socket-connect-rfc result=$connectResult socketIndex=${index[0]}")
                }
                if (connectResult < 0 || index[0] < 0) {
                    throw IOException("ANWBT_SocketConnect failed ret=$connectResult index=${index[0]}")
                }
                connectedSocketIndex = index[0]
                socketConnected = true
                val stream = Ix35AnwSocketStream(appContext, remote, serviceConnection, index[0], socketCallback, ::report)
                socketCallback.attach(index[0]) { bytes, length -> stream.enqueue(bytes, length) }
                report("event=socket-connected result=$connectResult socketIndex=${index[0]} elapsedMs=${(System.nanoTime() - connectStartedNanos) / 1_000_000L}")
                return stream
            } catch (e: Exception) {
                report("event=connect-failed stage=$stage error=${e.javaClass.simpleName} elapsedMs=${(System.nanoTime() - connectStartedNanos) / 1_000_000L}")
                if (socketConnected) {
                    runCatching {
                        val result = transact(binder, TRANSACTION_SOCKET_DISCONNECT, write = { writeInt(connectedSocketIndex) })
                        report("event=failure-cleanup-disconnect result=$result")
                    }.onFailure { report("event=failure-cleanup-disconnect-failed error=${it.javaClass.simpleName}") }
                }
                if (callbackRegistered) {
                    runCatching {
                        transactVoid(binder, TRANSACTION_UNREGISTER_SOCKET_CALLBACK) { writeStrongBinder(callback) }
                        report("event=failure-cleanup-callback-unregistered")
                    }.onFailure { report("event=failure-cleanup-callback-unregister-failed error=${it.javaClass.simpleName}") }
                }
                if (socketInitialized) runCatching {
                    val result = transact(binder, TRANSACTION_SOCKET_DENIT, write = {})
                    report("event=failure-cleanup-socket-deinit result=$result")
                }.onFailure { report("event=failure-cleanup-socket-deinit-failed error=${it.javaClass.simpleName}") }
                runCatching { appContext.unbindService(serviceConnection) }
                if (e is IOException) throw e
                throw IOException("ix35 ANW Socket connection failed", e)
            }
        }

        private fun transactVoid(remote: IBinder?, code: Int, write: Parcel.() -> Unit) {
            val target = remote ?: throw IOException("ANW Binder unavailable")
            val data = Parcel.obtain()
            val reply = Parcel.obtain()
            try {
                data.writeInterfaceToken(SERVICE_DESCRIPTOR)
                data.write()
                if (!target.transact(code, data, reply, 0)) throw RemoteException("Transaction $code rejected")
                reply.readException()
            } finally {
                reply.recycle()
                data.recycle()
            }
        }

        private fun transact(remote: IBinder?, code: Int, write: Parcel.() -> Unit, readTail: (Parcel) -> Unit = {}): Int {
            val target = remote ?: throw IOException("ANW Binder unavailable")
            val data = Parcel.obtain()
            val reply = Parcel.obtain()
            try {
                data.writeInterfaceToken(SERVICE_DESCRIPTOR)
                data.write()
                if (!target.transact(code, data, reply, 0)) throw RemoteException("Transaction $code rejected")
                reply.readException()
                val result = reply.readInt()
                readTail(reply)
                return result
            } finally {
                reply.recycle()
                data.recycle()
            }
        }

        private fun uuidToBytes(value: String): ByteArray {
            val uuid = UUID.fromString(value)
            val raw = ByteArray(16)
            val msb = uuid.mostSignificantBits
            val lsb = uuid.leastSignificantBits
            for (i in 0 until 8) raw[i] = (msb ushr ((7 - i) * 8)).toByte()
            for (i in 0 until 8) raw[i + 8] = (lsb ushr ((7 - i) * 8)).toByte()
            return raw
        }

        private fun uuidToGuidBytes(value: String): ByteArray {
            val raw = uuidToBytes(value)
            return byteArrayOf(raw[3], raw[2], raw[1], raw[0], raw[5], raw[4], raw[7], raw[6]) + raw.copyOfRange(8, 16)
        }
    }
}
