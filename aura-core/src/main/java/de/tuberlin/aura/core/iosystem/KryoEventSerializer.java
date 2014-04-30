package de.tuberlin.aura.core.iosystem;


import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import de.tuberlin.aura.core.task.common.TaskExecutionUnit;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

// TODO: set unique id for serializer!
// TODO: What for a unique id?

/**
 *
 */
public final class KryoEventSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(KryoEventSerializer.class);

    // Disallow instantiation.
    private KryoEventSerializer() {}

    // ---------------------------------------------------
    // Kryo Inbound- & Outbound-Handler.
    // ---------------------------------------------------

    public static final class KryoInboundHandler extends ChannelInboundHandlerAdapter {

        private Kryo kryo;

        public KryoInboundHandler(final TransferBufferEventSerializer transferBufferEventSerializer) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer());
            kryo.register(IOEvents.TransferBufferEvent.class, transferBufferEventSerializer);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            final ByteBuf buffer = (ByteBuf) msg;
            try {
                final Object event = kryo.readClassAndObject(new Input(new ByteBufInputStream(buffer)));
                ctx.fireChannelRead(event);
            } finally {
                buffer.release();
            }
        }
    }

    public static final class KryoOutboundHandler extends ChannelOutboundHandlerAdapter {

        private Kryo kryo;

        public KryoOutboundHandler(final TransferBufferEventSerializer transferBufferEventSerializer) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer());
            kryo.register(IOEvents.TransferBufferEvent.class, transferBufferEventSerializer);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            final ByteBuf ioBuffer = ctx.alloc().buffer();
            ioBuffer.writeInt(0);
            kryo.writeClassAndObject(new Output(new ByteBufOutputStream(ioBuffer)), msg);
            final int size = ioBuffer.writerIndex() - 4;
            ioBuffer.writerIndex(0).writeInt(size).writerIndex(size + 4);
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
            output.flush();
        }

        @Override
        public IOEvents.BaseIOEvent read(Kryo kryo, Input input, Class<IOEvents.BaseIOEvent> type) {
            return new IOEvents.BaseIOEvent(input.readString());
        }
    }

    public static class DataIOEventSerializer extends Serializer<IOEvents.DataIOEvent> {

        public DataIOEventSerializer() {}

        @Override
        public void write(Kryo kryo, Output output, IOEvents.DataIOEvent dataIOEvent) {

            // TODO: Use UUIDSerializer

            output.writeString(dataIOEvent.type);
            output.writeLong(dataIOEvent.srcTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.srcTaskID.getLeastSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getMostSignificantBits());
            output.writeLong(dataIOEvent.dstTaskID.getLeastSignificantBits());
            output.flush();
        }

        @Override
        public IOEvents.DataIOEvent read(Kryo kryo, Input input, Class<IOEvents.DataIOEvent> type) {
            final String eventType = input.readString();
            final UUID src = new UUID(input.readLong(), input.readLong());
            final UUID dst = new UUID(input.readLong(), input.readLong());
            return new IOEvents.DataIOEvent(eventType, src, dst);
        }
    }

    public static class TransferBufferEventSerializer extends Serializer<IOEvents.TransferBufferEvent> {

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
            output.writeBytes(transferBufferEvent.buffer.memory, transferBufferEvent.buffer.baseOffset, allocator.getBufferSize());
            output.flush();
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

            final MemoryManager.MemoryView buffer = allocator.alloc();
            input.readBytes(buffer.memory, buffer.baseOffset, allocator.getBufferSize());
            return new IOEvents.TransferBufferEvent(msgID, src, dst, buffer);
        }
    }
}
