package de.tuberlin.aura.core.iosystem;


import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;

import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.BufferCallback;
import de.tuberlin.aura.core.memory.IAllocator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import de.tuberlin.aura.core.task.common.TaskExecutionUnit;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

public final class KryoEventSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(KryoEventSerializer.class);

    // Disallow instantiation.
    private KryoEventSerializer() {}

    // ---------------------------------------------------
    // Netty specific stuff.
    // ---------------------------------------------------

    public static LengthFieldBasedFrameDecoder LENGTH_FIELD_DECODER() {
        return new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4);
    }

    public static ChannelInboundHandlerAdapter KRYO_INBOUND_HANDLER(final TaskExecutionManager taskExecutionManager) {
        return new KryoInboundHandler(taskExecutionManager);
    }

    public static ChannelOutboundHandlerAdapter KRYO_OUTBOUND_HANDLER() {
        return new KryoOutboundHandler();
    }

    // ---------------------------------------------------
    // Kryo Inbound- & Outbound-Handler.
    // ---------------------------------------------------

    private static final class KryoInboundHandler extends ChannelInboundHandlerAdapter {

        private Kryo kryo;

        private IAllocator allocator;

        private final TaskExecutionManager executionManager;

        private MemoryView userSpaceBuffer;

        private final AtomicInteger pendingCallbacks = new AtomicInteger(0);

        private final Object lock = new Object();

        public KryoInboundHandler(TaskExecutionManager executionManager) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), IOConfig.IO_DATA_EVENT_ID);
            kryo.register(IOEvents.TransferBufferEvent.class, new TransferBufferEventSerializer(this), IOConfig.IO_TRANSFER_EVENT_ID);
            this.executionManager = executionManager;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            final ByteBuf ioBuffer = (ByteBuf) msg;
            LOG.warn("entry " + ctx);
            try {
                final Input input = new UnsafeMemoryInput(ioBuffer.memoryAddress(), IOConfig.MAX_EVENT_SIZE);
                ioBuffer.order(ByteOrder.nativeOrder());
                final Registration reg = kryo.readClass(input);

                switch (reg.getId()) {
                    case IOConfig.IO_DATA_EVENT_ID: {
                        final Object event = kryo.readObject(input, reg.getType());
                        if (allocator == null && executionManager != null) {
                            bindAllocator(((IOEvents.DataIOEvent) event).srcTaskID, ((IOEvents.DataIOEvent) event).dstTaskID);
                            LOG.info(((IOEvents.DataIOEvent) event).srcTaskID + " bound to allocator: " + allocator);
                        }
                        ctx.fireChannelRead(event);
                        break;
                    }
                    case IOConfig.IO_TRANSFER_EVENT_ID: {
                        // get buffer
                        MemoryView view = allocator.alloc();
                        if (view == null) {
                            synchronized (lock) {
                                view = allocator.alloc(new Callback(ioBuffer, ctx));
                                if (view == null) {
                                    if (pendingCallbacks.incrementAndGet() == 1) {
                                        ctx.channel().config().setAutoRead(false);
                                    }
                                    ReferenceCountUtil.retain(ioBuffer);
                                    LOG.warn("view == null " + ctx);
                                } else {
                                    userSpaceBuffer = view;
                                    Object event = kryo.readObject(input, reg.getType());
                                    ctx.fireChannelRead(event);
                                }
                            }
                        } else {
                            userSpaceBuffer = view;
                            Object event = kryo.readObject(input, reg.getType());
                            ctx.fireChannelRead(event);
                        }

                        break;
                    }

                    default:
                        throw new IllegalStateException("Unregistered Class.");
                }
            } finally {
                ioBuffer.release();
            }
        }

        private class Callback implements BufferCallback {


            private final ByteBuf pendingBuffer;

            private final ChannelHandlerContext ctx;

            Callback(final ByteBuf pendingBuffer, ChannelHandlerContext ctx) {
                this.pendingBuffer = pendingBuffer;
                this.ctx = ctx;
            }

            @Override
            public void bufferReader(MemoryView buffer) {
                synchronized (lock) {
                    try {
                        userSpaceBuffer = buffer;
                        final Input input = new UnsafeMemoryInput(pendingBuffer.memoryAddress(), IOConfig.MAX_EVENT_SIZE);
                        Object event = kryo.readClassAndObject(input);
                        ctx.fireChannelRead(event);
                        LOG.warn("callback to ctx -> " + ctx + " with buffer " + buffer + "[ " + pendingBuffer.nioBuffer() + " ]");
                    } finally {
                        pendingBuffer.release();
                    }
                }
                if (pendingCallbacks.decrementAndGet() == 0) {
                    ctx.channel().config().setAutoRead(true);
                }
            }
        }

        public MemoryView getBuffer() {
            return userSpaceBuffer;
        }

        private void bindAllocator(UUID src, UUID dst) {
            final TaskExecutionManager tem = executionManager;
            final TaskExecutionUnit executionUnit = tem.findTaskExecutionUnitByTaskID(dst);
            final TaskDriverContext taskDriverContext = executionUnit.getCurrentTaskDriverContext();
            final DataConsumer dataConsumer = taskDriverContext.getDataConsumer();
            final int gateIndex = dataConsumer.getInputGateIndexFromTaskID(src);
            IAllocator allocatorGroup = executionUnit.getInputAllocator();

            // -------------------- STUPID HOT FIX --------------------

            if (taskDriverContext.taskBindingDescriptor.inputGateBindings.size() == 1) {
                allocator = allocatorGroup;
            } else {
                if (taskDriverContext.taskBindingDescriptor.inputGateBindings.size() == 2) {
                    if (gateIndex == 0) {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(0),
                                                                       ((BufferAllocatorGroup) allocatorGroup).getAllocator(1)));
                    } else {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(2),
                                                                       ((BufferAllocatorGroup) allocatorGroup).getAllocator(3)));
                    }
                } else {
                    throw new IllegalStateException("Not supported more than two input gates.");
                }
            }

            // -------------------- STUPID HOT FIX --------------------
        }

    }

    private static final class KryoOutboundHandler extends ChannelOutboundHandlerAdapter {

        private Kryo kryo;

        public KryoOutboundHandler() {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), IOConfig.IO_DATA_EVENT_ID);
            kryo.register(IOEvents.TransferBufferEvent.class, new TransferBufferEventSerializer(null), IOConfig.IO_TRANSFER_EVENT_ID);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            final ByteBuf ioBuffer = ctx.alloc().buffer(IOConfig.MAX_EVENT_SIZE, IOConfig.MAX_EVENT_SIZE);
            UnsafeMemoryOutput output = new UnsafeMemoryOutput(ioBuffer.memoryAddress(), IOConfig.MAX_EVENT_SIZE);
            output.order(ByteOrder.nativeOrder());
            // leave space for size info
            output.setPosition(4);
            kryo.writeClassAndObject(output, msg);
            final int size = output.position() - 4;
            // write size of event
            ioBuffer.writeInt(size).writerIndex(size + 4);
            ctx.write(ioBuffer, promise);
        }
    }

    // ---------------------------------------------------
    // Kryo Serializer.
    // ---------------------------------------------------


    private static class BaseIOEventSerializer extends Serializer<IOEvents.BaseIOEvent> {

        @Override
        public void write(Kryo kryo, Output output, IOEvents.BaseIOEvent baseIOEvent) {
            output.writeString(baseIOEvent.type);
        }

        @Override
        public IOEvents.BaseIOEvent read(Kryo kryo, Input input, Class<IOEvents.BaseIOEvent> type) {
            return new IOEvents.BaseIOEvent(input.readString());
        }
    }

    private static class DataIOEventSerializer extends Serializer<IOEvents.DataIOEvent> {

        public DataIOEventSerializer() {}

        @Override
        public void write(Kryo kryo, Output output, IOEvents.DataIOEvent dataIOEvent) {
            output.writeString(dataIOEvent.type);
            output.writeLong(dataIOEvent.srcTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.srcTaskID.getLeastSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getLeastSignificantBits());
        }

        @Override
        public IOEvents.DataIOEvent read(Kryo kryo, Input input, Class<IOEvents.DataIOEvent> type) {
            final String eventType = input.readString();
            final UUID src = new UUID(input.readLong(), input.readLong());
            final UUID dst = new UUID(input.readLong(), input.readLong());
            return new IOEvents.DataIOEvent(eventType, src, dst);
        }
    }

    private static class TransferBufferEventSerializer extends Serializer<IOEvents.TransferBufferEvent> {

        private final KryoInboundHandler handler;

        public TransferBufferEventSerializer(KryoInboundHandler handler) {
            this.handler = handler;
        }

        @Override
        public void write(Kryo kryo, Output output, IOEvents.TransferBufferEvent transferBufferEvent) {
            // TODO: Use UUIDSerializer
            output.writeLong(transferBufferEvent.srcTaskID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.srcTaskID.getLeastSignificantBits());
            output.writeLong(transferBufferEvent.dstTaskID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.dstTaskID.getLeastSignificantBits());
            output.writeLong(transferBufferEvent.messageID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.messageID.getLeastSignificantBits());
            ((UnsafeMemoryOutput) output).writeBytes((Object) transferBufferEvent.buffer.memory,
                                                     transferBufferEvent.buffer.baseOffset,
                                                     transferBufferEvent.buffer.size());

            transferBufferEvent.buffer.free();
            LOG.info("free");
   }

        @Override
        public IOEvents.TransferBufferEvent read(Kryo kryo, Input input, Class<IOEvents.TransferBufferEvent> type) {

            final UUID src = new UUID(input.readLong(), input.readLong());
            final UUID dst = new UUID(input.readLong(), input.readLong());
            final UUID msgID = new UUID(input.readLong(), input.readLong());

            final MemoryView buffer = handler.getBuffer();
            input.readBytes(buffer.memory, buffer.baseOffset, buffer.size());
            return new IOEvents.TransferBufferEvent(msgID, src, dst, buffer);
        }
    }
}
