package de.tuberlin.aura.core.iosystem;


import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;

import de.tuberlin.aura.core.memory.MemoryManager;
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

    public static ChannelInboundHandlerAdapter KRYO_INBOUND_HANDLER(final MemoryManager.Allocator allocator,
                                                                    final TaskExecutionManager taskExecutionManager) {
        return new KryoInboundHandler(new TransferBufferEventSerializer(allocator, taskExecutionManager));
    }

    public static ChannelOutboundHandlerAdapter KRYO_OUTBOUND_HANDLER(final MemoryManager.Allocator allocator,
                                                                      final TaskExecutionManager taskExecutionManager) {
        return new KryoOutboundHandler(new TransferBufferEventSerializer(allocator, taskExecutionManager));
    }

    // ---------------------------------------------------
    // Kryo Inbound- & Outbound-Handler.
    // ---------------------------------------------------

    private static final class KryoInboundHandler extends ChannelInboundHandlerAdapter {

        private Kryo kryo;

        public KryoInboundHandler(final TransferBufferEventSerializer transferBufferEventSerializer) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), IOConfig.IO_DATA_EVENT_ID);
            kryo.register(IOEvents.TransferBufferEvent.class, transferBufferEventSerializer, IOConfig.IO_TRANSFER_EVENT_ID);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            final ByteBuf buffer = (ByteBuf) msg;
            try {
                Input input = new UnsafeMemoryInput(buffer.memoryAddress(), IOConfig.MAX_EVENT_SIZE);
                buffer.order(ByteOrder.nativeOrder());
                final Object event = kryo.readClassAndObject(input);
                ctx.fireChannelRead(event);
            } finally {
                buffer.release();
            }
        }
    }

    private static final class KryoOutboundHandler extends ChannelOutboundHandlerAdapter {

        private Kryo kryo;

        public KryoOutboundHandler(final TransferBufferEventSerializer transferBufferEventSerializer) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer(), IOConfig.IO_DATA_EVENT_ID);
            kryo.register(IOEvents.TransferBufferEvent.class, transferBufferEventSerializer, IOConfig.IO_TRANSFER_EVENT_ID);
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

        public DataIOEventSerializer() {
        }

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

        private MemoryManager.Allocator allocator;

        private TaskExecutionManager executionManager;

        public TransferBufferEventSerializer(final MemoryManager.Allocator allocator, final TaskExecutionManager executionManager) {
            this.allocator = allocator;
            this.executionManager = executionManager;
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
           transferBufferEvent.buffer.baseOffset, allocator.getBufferSize());

            // TODO: executionManager is null here
            transferBufferEvent.buffer.free();
        }

        @Override
        public IOEvents.TransferBufferEvent read(Kryo kryo, Input input, Class<IOEvents.TransferBufferEvent> type) {
            final UUID src = new UUID(input.readLong(), input.readLong());
            final UUID dst = new UUID(input.readLong(), input.readLong());
            final UUID msgID = new UUID(input.readLong(), input.readLong());

            if (allocator == null) {
                final TaskExecutionManager tem = executionManager;
                final TaskExecutionUnit executionUnit = tem.findTaskExecutionUnitByTaskID(dst);
                final TaskDriverContext taskDriverContext = executionUnit.getCurrentTaskDriverContext();
                final DataConsumer dataConsumer = taskDriverContext.getDataConsumer();
                final int gateIndex = dataConsumer.getInputGateIndexFromTaskID(src);
                MemoryManager.BufferAllocatorGroup allocatorGroup = executionUnit.getInputAllocator();

                // -------------------- STUPID HOT FIX --------------------

                if (taskDriverContext.taskBindingDescriptor.inputGateBindings.size() == 1) {
                    allocator = allocatorGroup;
                } else {
                    if (taskDriverContext.taskBindingDescriptor.inputGateBindings.size() == 2) {
                        if (gateIndex == 0) {
                            allocator =
                                    new MemoryManager.BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                                                                           Arrays.asList(allocatorGroup.getAllocator(0),
                                                    allocatorGroup.getAllocator(1)));
                        } else {
                            allocator =
                                    new MemoryManager.BufferAllocatorGroup(allocatorGroup.getBufferSize(),
                    Arrays.asList(allocatorGroup.getAllocator(2),
                                                    allocatorGroup.getAllocator(3)));
                        }
                    } else {
                        throw new IllegalStateException("Not supported more than two input gates.");
                    }
                }

                // -------------------- STUPID HOT FIX --------------------
            }

            // TODO: Remove expensive logging
            // Descriptors.TaskDescriptor td =
            // executionManager.findTaskExecutionUnitByTaskID(dst).getCurrentTaskDriverContext().taskDescriptor;
            // LOG.debug("Read for {} {}", td.name, td.taskIndex);
            final MemoryManager.MemoryView buffer = allocator.alloc();
            // LOG.debug("Read for {} {} -> memory allocated", td.name, td.taskIndex);

            input.readBytes(buffer.memory, buffer.baseOffset, allocator.getBufferSize());

            return new IOEvents.TransferBufferEvent(msgID, src, dst, buffer);
        }
    }
}
