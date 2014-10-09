package de.tuberlin.aura.core.iosystem;


import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;

import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.spi.IBufferCallback;
import de.tuberlin.aura.core.taskmanager.spi.IDataConsumer;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;
import de.tuberlin.aura.core.taskmanager.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.taskmanager.spi.ITaskExecutionUnit;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

public final class SerializationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SerializationHandler.class);

    private SerializationHandler() {}

    /**
     * Splits the ByteBuf into events depending on the length field (first 4 bytes).
     * 
     * @return a frame decoder splitting the byte buf into events.
     */
    public static LengthFieldBasedFrameDecoder LENGTH_FIELD_DECODER() {
        return new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4);
    }

    /**
     * Inbound handler that de-serializes
     * {@link de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent}.
     * 
     * @param taskExecutionManager the taskmanager execution manager this handler is bound to
     * @param config
     * @return inbound de-serialization handler for
     *         {@link de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent}
     */
    public static ChannelInboundHandlerAdapter KRYO_INBOUND_HANDLER(final ITaskExecutionManager taskExecutionManager, IConfig config) {
        return new KryoDeserializationHandler(taskExecutionManager, config);
    }

    /**
     * Outbound handler that serializes {@link de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent}.
     * 
     * @return outbound serialization handler
     * @param config
     */
    public static ChannelOutboundHandlerAdapter KRYO_OUTBOUND_HANDLER(IConfig config) {
        return new KryoOutboundHandler(config);
    }

    /**
     * An DataIOEvent waiting for a callback to finish before it can be handled.
     */
    private static class PendingEvent {

        /**
         * The index of the callback that has to handle the {@link this#event}.
         */
        public final long index;

        /**
         * The pending event.
         */
        public final Object event;

        public PendingEvent(final long index, final Object event) {
            this.index = index;
            this.event = event;
        }
    }

    /**
     *
     */
    private static final class KryoDeserializationHandler extends ChannelInboundHandlerAdapter {

        private final int dataEventID;

        private final int transferEventID;

        private final IConfig config;

        private final Kryo kryo;

        private IAllocator allocator;

        private final ITaskExecutionManager executionManager;

        private MemoryView deseralizationBuffer;

        private int callbackID = 0;

        private int pendingCallbacks = 0;

        // private final Object lock = new Object();

        private final LinkedList<PendingEvent> pendingObjects = new LinkedList<>();

        public KryoDeserializationHandler(ITaskExecutionManager executionManager, IConfig config) {
            this.config = config;
            this.dataEventID = config.getInt("event.data.id");
            this.transferEventID = config.getInt("event.transfer.id");
            this.kryo = new Kryo();
            this.kryo.register(byte[].class);
            this.kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), this.dataEventID);
            this.kryo.register(IOEvents.TransferBufferEvent.class, new TransferBufferEventSerializer(this), this.transferEventID);
            this.executionManager = executionManager;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            final ByteBuf ioBuffer = (ByteBuf) msg;
            // final ByteBuf ioBuffer = ioBufferTMP.copy();
            try {
                final Input input = new UnsafeMemoryInput(ioBuffer.memoryAddress(), config.getInt("event.size.max"));
                // final Input input = new Input(ioBuffer.array());//, IOConfig.MAX_EVENT_SIZE);

                ioBuffer.order(ByteOrder.nativeOrder());
                final Registration reg = kryo.readClass(input);

                int id = reg.getId();
                if (id == dataEventID) {
                    final Object event = kryo.readObject(input, reg.getType());
                    // bind the allocator on first event, which must be a connected event
                    if (allocator == null && executionManager != null) {
                        bindAllocator(((IOEvents.DataIOEvent) event).srcTaskID, ((IOEvents.DataIOEvent) event).dstTaskID);
                    }
                    // synchronized (lock) {
                    if (pendingCallbacks >= 1) {
                        pendingObjects.offer(new PendingEvent(callbackID, event));
                    } else {
                        ctx.fireChannelRead(event);
                    }
                    // }
                } else if (id == transferEventID) {
                    // get buffer
                    // synchronized (lock) {
                    MemoryView view = allocator.alloc(new Callback(ioBuffer, ctx));
                    if (view == null) {
                        if (++pendingCallbacks == 1) {
                            ctx.channel().config().setAutoRead(false);
                        }
                        ReferenceCountUtil.retain(ioBuffer);
                    } else {
                        callbackID--;
                        deseralizationBuffer = view;
                        Object event = kryo.readObject(input, reg.getType());
                        ctx.fireChannelRead(event);
                    }
                    // }
                } else {
                    throw new IllegalStateException("Unregistered Class.");
                }
            } finally {
                ioBuffer.release();
            }
        }

        /**
         *
         */
        private class Callback implements IBufferCallback {

            private final ByteBuf pendingBuffer;

            private final ChannelHandlerContext ctx;

            private final long index;

            Callback(final ByteBuf pendingBuffer, ChannelHandlerContext ctx) {
                this.pendingBuffer = pendingBuffer;
                this.ctx = ctx;
                this.index = ++callbackID;
            }

            @Override
            public void bufferReader(final MemoryView buffer) {
                ctx.channel().eventLoop().execute(new Runnable() {

                    @Override
                    public void run() {
                        // synchronized (lock) {
                        try {
                            deseralizationBuffer = buffer;
                            final Input input = new UnsafeMemoryInput(pendingBuffer.memoryAddress(), config.getInt("event.size.max"));
                            // final Input input = new Input(pendingBuffer.array());
                            Object event = kryo.readClassAndObject(input);
                            ctx.fireChannelRead(event);
                            for (Iterator<PendingEvent> itr = pendingObjects.iterator(); itr.hasNext();) {
                                PendingEvent obj = itr.next();
                                if (obj.index == index) {
                                    ctx.fireChannelRead(obj.event);
                                    itr.remove();
                                } else if (obj.index <= index) {
                                    LOG.warn("obj.index < index " + obj.event);
                                    ctx.fireChannelRead(obj.event);
                                    itr.remove();
                                } else {
                                    break;
                                }
                            }
                        } finally {
                            pendingBuffer.release();
                        }
                        if (--pendingCallbacks == 0) {
                            ctx.channel().config().setAutoRead(true);
                            ctx.pipeline().read();
                        }
                        // }
                    }
                });
            }
        }

        public MemoryView getBuffer() {
            return deseralizationBuffer;
        }

        private void bindAllocator(UUID src, UUID dst) {
            final ITaskExecutionManager tem = executionManager;

            ITaskExecutionUnit executionUnit = tem.getExecutionUnitByTaskID(dst);
            final ITaskRuntime taskDriver = executionUnit.getRuntime();
            final IDataConsumer dataConsumer = taskDriver.getConsumer();
            final int gateIndex = dataConsumer.getInputGateIndexFromTaskID(src);

            IAllocator allocatorGroup = executionUnit.getInputAllocator();

            // -------------------- STUPID HOT FIX --------------------

            if (taskDriver.getBindingDescriptor().inputGateBindings.size() == 1) {
                allocator = allocatorGroup;
            } else {
                if (taskDriver.getBindingDescriptor().inputGateBindings.size() == 2) {
                    if (gateIndex == 0) {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(0)));
                    } else {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(1)));
                    }
                } else {
                    throw new IllegalStateException("Not supported more than two input gates.");
                }
            }

            // -------------------- STUPID HOT FIX --------------------
        }
    }

    /**
     *
     */
    private static final class KryoOutboundHandler extends ChannelOutboundHandlerAdapter {

        private IConfig config;

        private Kryo kryo;

        public KryoOutboundHandler(IConfig config) {
            this.config = config;
            this.kryo = new Kryo();
            this.kryo.register(byte[].class);
            this.kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), config.getInt("event.data.id"));
            this.kryo.register(IOEvents.TransferBufferEvent.class, new TransferBufferEventSerializer(null), config.getInt("event.transfer.id"));
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            // LOG.warn("write");
            final ByteBuf ioBuffer = ctx.alloc().buffer(config.getInt("event.size.max"), config.getInt("event.size.max"));
            UnsafeMemoryOutput output = new UnsafeMemoryOutput(ioBuffer.memoryAddress(), config.getInt("event.size.max"));
            // Output output = new Output(ioBuffer.array());
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

    /**
     *
     */
    public static final class LocalTransferBufferCopyHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        private long callbackID = 0;

        private IAllocator allocator;

        private final ITaskExecutionManager executionManager;

        private int pendingCallbacks = 0;

        // private final Object lock = new Object();

        private final LinkedList<PendingEvent> pendingObjects = new LinkedList<>();

        public LocalTransferBufferCopyHandler(ITaskExecutionManager executionManager) {
            this.executionManager = executionManager;
        }

        @Override
        public void channelRead0(final ChannelHandlerContext ctx, IOEvents.DataIOEvent msg) throws Exception {

            switch (msg.type) {
                case IOEvents.DataEventType.DATA_EVENT_BUFFER: {
                    // synchronized (lock) {
                    MemoryView view = allocator.alloc(new Callback((IOEvents.TransferBufferEvent) msg, ctx));
                    if (view == null) {
                        if (++pendingCallbacks == 1) {
                            ctx.channel().config().setAutoRead(false);
                        }
                    } else {
                        callbackID--;
                        IOEvents.TransferBufferEvent event = (IOEvents.TransferBufferEvent) msg;
                        System.arraycopy(event.buffer.memory, event.buffer.baseOffset, view.memory, view.baseOffset, event.buffer.size());
                        event.buffer.free();
                        IOEvents.TransferBufferEvent copy = new IOEvents.TransferBufferEvent(event.srcTaskID, event.dstTaskID, view);
                        ctx.fireChannelRead(copy);
                    }
                    // }
                    break;
                }

                default: {
                    if (allocator == null && executionManager != null) {
                        bindAllocator(msg.srcTaskID, msg.dstTaskID);
                    }
                    // synchronized (lock) {
                    if (pendingCallbacks >= 1) {
                        pendingObjects.offer(new PendingEvent(callbackID, msg));
                    } else {
                        ctx.fireChannelRead(msg);
                    }
                    // }
                    break;
                }
            }
        }

        /**
         *
         */
        private class Callback implements IBufferCallback {

            private final IOEvents.TransferBufferEvent transferBufferEvent;

            private final ChannelHandlerContext ctx;

            private final long index;

            Callback(final IOEvents.TransferBufferEvent transferBufferEvent, ChannelHandlerContext ctx) {
                this.transferBufferEvent = transferBufferEvent;
                this.ctx = ctx;
                this.index = ++callbackID;
            }

            @Override
            public void bufferReader(final MemoryView buffer) {
                ctx.channel().eventLoop().execute(new Runnable() {

                    @Override
                    public void run() {
                        // synchronized (lock) {
                        System.arraycopy(transferBufferEvent.buffer.memory,
                                         transferBufferEvent.buffer.baseOffset,
                                         buffer.memory,
                                         buffer.baseOffset,
                                         transferBufferEvent.buffer.size());
                        transferBufferEvent.buffer.free();
                        IOEvents.TransferBufferEvent copy =
                                new IOEvents.TransferBufferEvent(transferBufferEvent.srcTaskID, transferBufferEvent.dstTaskID, buffer);
                        ctx.fireChannelRead(copy);
                        for (Iterator<PendingEvent> itr = pendingObjects.iterator(); itr.hasNext();) {
                            PendingEvent obj = itr.next();
                            if (obj.index == index) {
                                ctx.fireChannelRead(obj.event);
                                itr.remove();
                            } else if (obj.index <= index) {
                                LOG.warn("obj.index < index " + obj.event);
                                ctx.fireChannelRead(obj.event);
                                itr.remove();
                            } else {
                                break;
                            }
                        }
                        if (--pendingCallbacks == 0) {
                            ctx.channel().config().setAutoRead(true);
                            ctx.pipeline().read();
                        }
                        // }
                    }
                });
            }
        }

        private void bindAllocator(UUID src, UUID dst) {
            final ITaskExecutionManager tem = executionManager;
            final ITaskExecutionUnit executionUnit = tem.getExecutionUnitByTaskID(dst);
            final ITaskRuntime taskDriver = executionUnit.getRuntime();
            final IDataConsumer dataConsumer = taskDriver.getConsumer();
            final int gateIndex = dataConsumer.getInputGateIndexFromTaskID(src);
            IAllocator allocatorGroup = executionUnit.getInputAllocator();

            // -------------------- STUPID HOT FIX --------------------

            if (taskDriver.getBindingDescriptor().inputGateBindings.size() == 1) {
                allocator = allocatorGroup;
            } else {
                if (taskDriver.getBindingDescriptor().inputGateBindings.size() == 2) {
                    if (gateIndex == 0) {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(0)));
                    } else {
                        allocator =
                                new BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                         Arrays.asList(((BufferAllocatorGroup) allocatorGroup).getAllocator(1)));
                    }
                } else {
                    throw new IllegalStateException("Not supported more than two input gates.");
                }
            }

            // -------------------- STUPID HOT FIX --------------------
        }

    }

    // ---------------------------------------------------
    // Kryo Serializer.
    // ---------------------------------------------------

    /**
     *
     */
    private static class DataIOEventSerializer extends Serializer<IOEvents.DataIOEvent> {

        public DataIOEventSerializer() {}

        @Override
        public void write(Kryo kryo, Output output, IOEvents.DataIOEvent dataIOEvent) {

            kryo.writeClass(output, (dataIOEvent.getPayload() != null) ? dataIOEvent.getPayload().getClass() : Object.class);
            kryo.writeObjectOrNull(output, dataIOEvent.getPayload(), (dataIOEvent.getPayload() != null)
                    ? dataIOEvent.getPayload().getClass()
                    : Object.class);

            output.writeString(dataIOEvent.type);
            output.writeLong(dataIOEvent.srcTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.srcTaskID.getLeastSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getLeastSignificantBits());
        }

        @Override
        public IOEvents.DataIOEvent read(Kryo kryo, Input input, Class<IOEvents.DataIOEvent> type) {
            Registration reg = kryo.readClass(input);
            final Object payload = kryo.readObjectOrNull(input, reg.getType());

            final String eventType = input.readString();
            final UUID src = new UUID(input.readLong(), input.readLong());
            final UUID dst = new UUID(input.readLong(), input.readLong());

            IOEvents.DataIOEvent event = new IOEvents.DataIOEvent(eventType, src, dst);
            event.setPayload(payload);

            return event;
        }
    }

    /**
     *
     */
    private static class TransferBufferEventSerializer extends Serializer<IOEvents.TransferBufferEvent> {

        private final KryoDeserializationHandler handler;

        public TransferBufferEventSerializer(KryoDeserializationHandler handler) {
            this.handler = handler;
        }

        @Override
        public void write(Kryo kryo, Output output, IOEvents.TransferBufferEvent transferBufferEvent) {

            output.writeBytes(transferBufferEvent.buffer.memory, transferBufferEvent.buffer.baseOffset, transferBufferEvent.buffer.size());

            output.writeLong(transferBufferEvent.srcTaskID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.srcTaskID.getLeastSignificantBits());
            output.writeLong(transferBufferEvent.dstTaskID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.dstTaskID.getLeastSignificantBits());
            output.writeLong(transferBufferEvent.messageID.getMostSignificantBits());
            output.writeLong(transferBufferEvent.messageID.getLeastSignificantBits());

            transferBufferEvent.buffer.free();
        }

        @Override
        public IOEvents.TransferBufferEvent read(Kryo kryo, Input input, Class<IOEvents.TransferBufferEvent> type) {

            final MemoryView buffer = handler.getBuffer();
            input.readBytes(buffer.memory, buffer.baseOffset, buffer.size());

            final UUID src = new UUID(input.readLong(false), input.readLong(false));
            final UUID dst = new UUID(input.readLong(false), input.readLong(false));
            final UUID msgID = new UUID(input.readLong(false), input.readLong(false));

            return new IOEvents.TransferBufferEvent(msgID, src, dst, buffer);
        }
    }
}
