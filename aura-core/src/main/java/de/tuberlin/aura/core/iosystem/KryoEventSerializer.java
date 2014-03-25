package de.tuberlin.aura.core.iosystem;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.UUID;

// TODO: set unique id for serializer!
// TODO: What for a unique id?

/**
 *
 */
public final class KryoEventSerializer {

    // Disallow instantiation.
    private KryoEventSerializer() {
    }

    // ---------------------------------------------------
    // Netty specific stuff.
    // ---------------------------------------------------

    public static LengthFieldBasedFrameDecoder getLengthDecoder() {
        return new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4);
    }

    // ---------------------------------------------------
    // Kryo Inbound- & Outbound-Handler.
    // ---------------------------------------------------

    public static final class KryoInboundHandler extends ChannelInboundHandlerAdapter {

        /*private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
            @Override
            protected Kryo initialValue() {
                return new Kryo();
            }
        };*/

        private Kryo kryo;

        public KryoInboundHandler(final TransferBufferEventSerializer transferBufferEventSerializer) {
            kryo = new Kryo();
            kryo.register(IOEvents.DataIOEvent.class, new DataIOEventSerializer());
            kryo.register(IOEvents.TransferBufferEvent.class, transferBufferEventSerializer);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            final ByteBuf buffer = (ByteBuf) msg;
            final Object event = kryo.readClassAndObject(new Input(new ByteBufInputStream(buffer)));
            buffer.release();
            ctx.fireChannelRead(event);
        }
    }

    public static final class KryoOutboundHandler extends ChannelOutboundHandlerAdapter {

        /*private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
            @Override
            protected Kryo initialValue() {
                return new Kryo();
            }
        };*/

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

        public DataIOEventSerializer() {
        }

        @Override
        public void write(Kryo kryo, Output output, IOEvents.DataIOEvent dataIOEvent) {
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


        public TransferBufferEventSerializer(final MemoryManager.Allocator allocator,
                                             final TaskExecutionManager executionManager) {
            this.allocator = allocator;
            this.executionManager = executionManager;
        }

        @Override
        public void write(Kryo kryo, Output output, IOEvents.TransferBufferEvent transferBufferEvent) {
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
                allocator = tem.findTaskExecutionUnitByTaskID(dst).getInputAllocator();
            }

            final MemoryManager.MemoryView buffer = allocator.alloc();
            input.readBytes(buffer.memory, buffer.baseOffset, allocator.getBufferSize());
            return new IOEvents.TransferBufferEvent(msgID, src, dst, buffer);
        }
    }
}
