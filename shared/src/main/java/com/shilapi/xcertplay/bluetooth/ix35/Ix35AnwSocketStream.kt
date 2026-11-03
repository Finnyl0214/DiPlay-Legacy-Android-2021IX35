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
    private val callback: SocketCallback
) : BlockingDuplexByteStream {
    private val lock = Object()
    private val writeLock = Object()
    private val incoming = ArrayDeque<ByteArray>()
    private var incomingBytes = 0
    private var closed = false
    private var peerClosed = false
    private var failure: IOException? = null

    private fun enqueue(bytes: ByteArray, length: Int) {
        if (length <= 0 || length > bytes.size) return
        synchronized(lock) {
            if (closed || peerClosed) return
            if (incomingBytes + length > MAX_PENDING_BYTES) {
                failure = IOException("ANW receive queue exceeded $MAX_PENDING_BYTES bytes")
                peerClosed = true
            } else {
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
                val result = transactInt(TRANSACTION_SOCKET_WRITE, {
                    writeInt(socketIndex)
                    writeByteArray(chunk)
                    writeInt(chunk.size)
                    writeInt(written.size)
                }, { reply -> reply.readIntArray(written) })
                if (result < 0) throw IOException("ANWBT_SocketWrite returned $result")
                val actual = written[0]
                if (actual <= 0 || actual > count) {
                    throw IOException("ANWBT_SocketWrite returned invalid written=${actual}, chunk=$count")
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
                    if (first.size <= maxBytes) {
                        incoming.removeFirst()
                        incomingBytes -= first.size
                        return first
                    }
                    val part = first.copyOf(maxBytes)
                    incoming.removeFirst()
                    incoming.addFirst(first.copyOfRange(maxBytes, first.size))
                    incomingBytes -= maxBytes
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
        callback.detach()
        try {
            transactVoid(TRANSACTION_UNREGISTER_SOCKET_CALLBACK) { writeStrongBinder(callback) }
        } catch (e: Exception) {
            Log.w(TAG, "ANW callback unregister failed", e)
        }
        try {
            transactInt(TRANSACTION_SOCKET_DISCONNECT, write = { writeInt(socketIndex) })
        } catch (e: Exception) {
            Log.w(TAG, "ANW socket disconnect failed", e)
        }
        // SocketInit allocates service-side state; release it after disconnect
        // on the normal close path as well as on connection failures.
        try {
            transactInt(TRANSACTION_SOCKET_DENIT)
        } catch (e: Exception) {
            Log.w(TAG, "ANW socket deinit failed", e)
        }
        try {
            context.unbindService(connection)
        } catch (e: Exception) {
            Log.w(TAG, "ANW service unbind failed", e)
        }
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

    private class SocketCallback : Binder(), IInterface {
        private val callbackLock = Any()
        private val pending = ArrayDeque<Pair<Int, ByteArray>>()
        private var pendingBytes = 0
        private var expectedIndex = -1
        private var receiver: ((ByteArray, Int) -> Unit)? = null

        init { attachInterface(this, CALLBACK_DESCRIPTOR) }
        override fun asBinder(): IBinder = this

        fun attach(index: Int, target: (ByteArray, Int) -> Unit) {
            val buffered = ArrayList<Pair<Int, ByteArray>>()
            synchronized(callbackLock) {
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
            synchronized(callbackLock) { receiver = null }
        }

        private fun dispatch(index: Int, bytes: ByteArray, length: Int) {
            if (length <= 0 || length > bytes.size) return
            val target: ((ByteArray, Int) -> Unit)?
            synchronized(callbackLock) {
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
                if (expectedIndex != index) return
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
        private const val CALLBACK_TRANSACTION_DATA_IND = 1
        private const val MAX_WRITE_CHUNK = 4096
        private const val MAX_PENDING_BYTES = 1024 * 1024
        private val EMPTY = ByteArray(0)
        private const val IX35_IAP2_GUID = "00000000-deca-fade-deca-deafdecacafe"

        /** Call from a worker thread; address must be the paired iPhone MAC. */
        @Throws(IOException::class)
        fun connect(context: Context, address: String): BlockingDuplexByteStream {
            require(address.matches(Regex("(?i)^[0-9a-f]{2}(:[0-9a-f]{2}){5}$"))) { "Invalid Bluetooth address" }
            val appContext = context.applicationContext
            val latch = CountDownLatch(1)
            var binder: IBinder? = null
            val serviceConnection = object : ServiceConnection {
                override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
                    binder = service
                    latch.countDown()
                }
                override fun onServiceDisconnected(name: ComponentName?) { binder = null }
            }
            val intent = Intent().setComponent(ComponentName(SERVICE_PACKAGE, SERVICE_CLASS))
            val bound = try {
                appContext.bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE)
            } catch (e: Exception) {
                throw IOException("Could not bind ix35 ANW service", e)
            }
            if (!bound) throw IOException("ix35 ANW service bindService returned false")
            var callback: SocketCallback? = null
            var socketInitialized = false
            try {
                if (!latch.await(5, TimeUnit.SECONDS)) throw IOException("Timed out binding ix35 ANW service")
                val remote = binder ?: throw IOException("ix35 ANW service returned a null Binder")
                if (!remote.isBinderAlive) throw IOException("ix35 ANW Binder is not alive")
                val socketCallback = SocketCallback()
                callback = socketCallback
                transactVoid(remote, TRANSACTION_REGISTER_SOCKET_CALLBACK) { writeStrongBinder(socketCallback) }
                val initResult = transact(remote, TRANSACTION_SOCKET_INIT, write = {})
                if (initResult < 0) throw IOException("ANWBT_SocketInit returned $initResult")
                socketInitialized = true
                val index = intArrayOf(-1)
                val guid = uuidToGuidBytes(IX35_IAP2_GUID)
                val connectResult = transact(
                    remote,
                    TRANSACTION_SOCKET_CONNECT,
                    write = {
                        writeInt(1)
                        writeString(address)
                        writeByteArray(guid)
                        writeInt(0)
                        writeLong(0L)
                        writeLong(0L)
                        writeInt(index.size)
                    },
                    readTail = { reply -> reply.readIntArray(index) }
                )
                if (connectResult < 0 || index[0] < 0) {
                    throw IOException("ANWBT_SocketConnect failed ret=$connectResult index=${index[0]}")
                }
                val stream = Ix35AnwSocketStream(appContext, remote, serviceConnection, index[0], socketCallback)
                socketCallback.attach(index[0]) { bytes, length -> stream.enqueue(bytes, length) }
                Log.i(TAG, "ix35 ANW Socket connected ret=$connectResult index=${index[0]}")
                return stream
            } catch (e: Exception) {
                runCatching { transactVoid(binder, TRANSACTION_UNREGISTER_SOCKET_CALLBACK) { writeStrongBinder(callback) } }
                if (socketInitialized) runCatching { transact(binder, TRANSACTION_SOCKET_DENIT, write = {}) }
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

        private fun uuidToGuidBytes(value: String): ByteArray {
            val uuid = UUID.fromString(value)
            val raw = ByteArray(16)
            val msb = uuid.mostSignificantBits
            val lsb = uuid.leastSignificantBits
            for (i in 0 until 8) raw[i] = (msb ushr ((7 - i) * 8)).toByte()
            for (i in 0 until 8) raw[i + 8] = (lsb ushr ((7 - i) * 8)).toByte()
            return byteArrayOf(raw[3], raw[2], raw[1], raw[0], raw[5], raw[4], raw[7], raw[6]) + raw.copyOfRange(8, 16)
        }
    }
}
