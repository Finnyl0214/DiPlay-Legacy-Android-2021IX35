package com.shilapi.xcertplay.usbbt

import java.io.Closeable
import java.util.concurrent.FutureTask
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import org.junit.Assert.*
import org.junit.Test

class HciEventDispatcherTest {
    private class Fixture : Closeable {
        val incoming = LinkedBlockingQueue<ByteArray>()
        val writes = LinkedBlockingQueue<ByteArray>()
        var onWrite: (ByteArray) -> Unit = {}
        val dispatcher = HciEventDispatcher(
            readEvent = { timeout -> incoming.poll(timeout.toLong(), TimeUnit.MILLISECONDS)?.let { HciProtocol.parseEvent(it) } },
            writeCommand = { packet, _ -> writes.offer(packet); onWrite(packet); packet.size },
        )
        override fun close() = dispatcher.close()
    }

    private fun complete(opcode: Int, status: Int = 0) =
        byteArrayOf(0x0E, 4, 1, opcode.toByte(), (opcode ushr 8).toByte(), status.toByte())
    private fun status(opcode: Int, status: Int = 0) =
        byteArrayOf(0x0F, 4, status.toByte(), 1, opcode.toByte(), (opcode ushr 8).toByte())
    private fun command(f: Fixture, opcode: Int, timeoutMs: Int = 1_000) =
        FutureTask { f.dispatcher.sendCommand(opcode, byteArrayOf(), timeoutMs) }.also { Thread(it).start() }

    @Test fun usbCommandCompleteIsDeliveredToItsCommand() {
        Fixture().use { f ->
            f.onWrite = { f.incoming.offer(complete(HciProtocol.OP_RESET)) }
            assertArrayEquals(byteArrayOf(0), f.dispatcher.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 1_000).getOrThrow())
            assertNull(f.dispatcher.pollEvent(10))
        }
    }

    @Test fun asyncLinkCommandsCompleteOnStatusAndKeepTheirFinalEvent() {
        for (opcode in listOf(
            HciProtocol.OP_CREATE_CONNECTION,
            HciProtocol.OP_ACCEPT_CONNECTION_REQUEST,
            HciProtocol.OP_DISCONNECT,
            HciProtocol.OP_AUTHENTICATION_REQUESTED,
            HciProtocol.OP_SET_CONNECTION_ENCRYPTION,
        )) {
            Fixture().use { f ->
                f.onWrite = { f.incoming.offer(status(opcode)); f.incoming.offer(byteArrayOf(0x03, 0)) }
                val result = f.dispatcher.sendCommand(opcode, byteArrayOf(), 1_000)
                assertTrue(
                    "opcode=0x${opcode.toString(16)} failed: ${result.exceptionOrNull()?.message}",
                    result.isSuccess,
                )
                val asyncEvent = f.dispatcher.pollEvent(1_000)
                assertNotNull("opcode=0x${opcode.toString(16)} lost its following async event", asyncEvent)
                assertEquals(0x03, asyncEvent?.code)
            }
        }
    }

    @Test fun failedStatusAndFailedCompleteDoNotReportSuccess() {
        Fixture().use { f ->
            f.onWrite = { f.incoming.offer(status(HciProtocol.OP_CREATE_CONNECTION, 0x0C)) }
            assertTrue(f.dispatcher.sendCommand(HciProtocol.OP_CREATE_CONNECTION, byteArrayOf(), 1_000)
                .exceptionOrNull()?.message.orEmpty().contains("status=0xc"))
            f.onWrite = { f.incoming.offer(complete(HciProtocol.OP_RESET, 1)) }
            assertTrue(f.dispatcher.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 1_000).isFailure)
        }
    }

    @Test fun eventConsumerCannotStealRepliesAndCommandsKeepAsyncEvents() {
        Fixture().use { f ->
            val pending = command(f, HciProtocol.OP_IO_CAPABILITY_REQUEST_REPLY)
            assertNotNull(f.writes.poll(1, TimeUnit.SECONDS))
            val event = FutureTask { f.dispatcher.pollEvent(1_000) }
            Thread(event).start()
            f.incoming.offer(byteArrayOf(0x33, 0))
            f.incoming.offer(complete(HciProtocol.OP_RESET)) // unrelated opcode
            f.incoming.offer(complete(HciProtocol.OP_IO_CAPABILITY_REQUEST_REPLY))
            assertEquals(0x33, event.get(2, TimeUnit.SECONDS)?.code)
            assertTrue(pending.get(2, TimeUnit.SECONDS).isSuccess)
        }
    }

    @Test fun synchronousReadStillRequiresCompleteAfterStatus() {
        Fixture().use { f ->
            val pending = command(f, HciProtocol.OP_READ_BD_ADDR)
            assertNotNull(f.writes.poll(1, TimeUnit.SECONDS))
            f.incoming.offer(status(HciProtocol.OP_READ_BD_ADDR))
            f.incoming.offer(byteArrayOf(0x33, 0))
            assertNotNull(f.dispatcher.pollEvent(1_000))
            assertFalse(pending.isDone)
            f.incoming.offer(complete(HciProtocol.OP_READ_BD_ADDR))
            assertTrue(pending.get(2, TimeUnit.SECONDS).isSuccess)
        }
    }

    @Test fun concurrentCommandsAreSerialized() {
        Fixture().use { f ->
            val first = command(f, HciProtocol.OP_RESET)
            assertNotNull(f.writes.poll(1, TimeUnit.SECONDS))
            val second = command(f, HciProtocol.OP_READ_BD_ADDR)
            assertNull(f.writes.poll(20, TimeUnit.MILLISECONDS))
            f.incoming.offer(complete(HciProtocol.OP_RESET))
            assertTrue(first.get(2, TimeUnit.SECONDS).isSuccess)
            assertArrayEquals(HciProtocol.encodeCommand(HciProtocol.OP_READ_BD_ADDR), f.writes.poll(1, TimeUnit.SECONDS))
            f.incoming.offer(complete(HciProtocol.OP_READ_BD_ADDR))
            assertTrue(second.get(2, TimeUnit.SECONDS).isSuccess)
        }
    }

    @Test fun closeReleasesPendingCommandAndRejectsNewCommands() {
        val f = Fixture()
        val pending = command(f, HciProtocol.OP_RESET, 10_000)
        assertNotNull(f.writes.poll(1, TimeUnit.SECONDS))
        f.close()
        assertTrue(pending.get(1, TimeUnit.SECONDS).isFailure)
        assertTrue(f.dispatcher.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 1_000).isFailure)
    }

    @Test fun failedControlTransferAndTimeoutFail() {
        HciEventDispatcher(readEvent = { Thread.sleep(20); null }, writeCommand = { _, _ -> -1 }).use {
            assertTrue(it.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 1_000).isFailure)
        }
        Fixture().use { f ->
            assertTrue(f.dispatcher.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 20).isFailure)
        }
    }

    @Test fun transportFailureReleasesPendingCommand() {
        val incoming = LinkedBlockingQueue<Boolean>()
        HciEventDispatcher(
            readEvent = { incoming.take(); throw java.io.IOException("USB disconnected") },
            writeCommand = { packet, _ -> incoming.offer(true); packet.size },
        ).use {
            assertEquals("USB disconnected", it.sendCommand(HciProtocol.OP_RESET, byteArrayOf(), 1_000).exceptionOrNull()?.message)
        }
    }
}
