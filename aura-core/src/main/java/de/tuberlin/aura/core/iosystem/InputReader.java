package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.iosystem.buffer.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

public class InputReader implements IChannelReader {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    protected final static Logger LOG = LoggerFactory.getLogger(AbstractReader.class);

    private final int bufferSize;
    private final IEventDispatcher dispatcher;
    /**
     * Queues the single channels write their data into. There are possibly more channels that write into a single queue. Each {@see
     * de.tuberlin.aura.core.task.gates.InputGate} as exactly one queue.
     */
    private final List<BufferQueue<IOEvents.DataIOEvent>> queues;
    /**
     * In order to write the data to the correct queue, we map (channel) -> queue index (in {@see queues})
     */
    private final Map<Channel, Integer> channelToQueueIndex;
    /**
     * In order to get the queue that belongs to the {@see InputGate}, we map (task id, gate index) -> queue index (in {@see queues})
     */
    private final Map<Pair<UUID, Integer>, Integer> gateToQueueIndex;
    private final Map<Pair<UUID, Integer>, List<Channel>> connectedChannels;

    private ByteBufAllocator contextLevelAllocator;

    public InputReader(IEventDispatcher dispatcher) {
        this(dispatcher, DEFAULT_BUFFER_SIZE);
    }

    public InputReader(IEventDispatcher dispatcher, int bufferSize) {
        this.dispatcher = dispatcher;

        // TODO: need for concurrent queue?! after start we just read values?
        queues = new LinkedList<>();
        channelToQueueIndex = new HashMap<>();
        gateToQueueIndex = new HashMap<>();

        connectedChannels = new HashMap<>();

        if ((bufferSize & (bufferSize - 1)) != 0) {
            LOG.warn("The given buffer size is not a power of two.");
            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }
    }

    @Override
    public <T extends Channel> void bind(final ConnectionType type, final SocketAddress address, final EventLoopGroup workerGroup) {

        ServerBootstrap bootstrap = type.bootStrap(workerGroup);
        bootstrap.childHandler(new ChannelInitializer<T>() {
            @Override
            public void initChannel(T ch) throws Exception {
                ch.pipeline()
                    .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
                    .addLast(new DataHandler())
                    .addLast(new EventHandler());
            }
        });

        try {
            // Bind and start to accept incoming connections.
            ChannelFuture f = bootstrap.bind(address).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future)
                    throws Exception {
                    if (future.isSuccess()) {
                        LOG.info("network server bound to address " + address);
                    } else {
                        LOG.error("bound attempt failed.", future.cause());
                        throw new IllegalStateException("could not start netty network server", future.cause());
                    }
                }
            }).sync();

            // TODO: add explicit close hook

        } catch (InterruptedException e) {
            LOG.error("failed to initialize network server", e);
            throw new IllegalStateException("failed to initialize network server", e);
        } finally {
            // TODO: shutdown event loop group eventLoopGroup.shutdownGracefully();
        }
    }

    @Override
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex) {
        if (!gateToQueueIndex.containsKey(new Pair<UUID, Integer>(taskID, gateIndex))) {
            return null;
        }
        return queues.get(gateToQueueIndex.get(new Pair<UUID, Integer>(taskID, gateIndex)));
    }

    @Override
    public void setInputQueue(final UUID srcTaskID, final Channel channel, final int gateIndex, final BufferQueue<IOEvents.DataIOEvent> queue) {

        Pair<UUID, Integer> index = new Pair<>(srcTaskID, gateIndex);
        final int queueIndex;
        if (gateToQueueIndex.containsKey(index)) {
            queueIndex = gateToQueueIndex.get(index);
        } else {
            this.queues.add(queue);
            queueIndex = queues.size() - 1;
            gateToQueueIndex.put(index, queueIndex);
        }
        channelToQueueIndex.put(channel, queueIndex);

        final List<Channel> channels;
        if (!connectedChannels.containsKey(index)) {
            channels = new ArrayList<>();
        } else {
            channels = connectedChannels.get(index);
        }
        channels.add(channel);
        connectedChannels.put(index, channels);
    }

    @Override
    public void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        connectedChannels.get(new Pair<UUID, Integer>(taskID, gateIndex)).get(channelIndex).writeAndFlush(event);
    }

    protected final class DataHandler extends SimpleChannelInboundHandler<IOEvents.DataBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataBufferEvent event) {
            //event.setChannel(ctx.channel());

            try {
                queues.get(channelToQueueIndex.get(ctx.channel())).offer(event);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private final class EventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataIOEvent event)
            throws Exception {

            //LOG.info("got event: " + event.type);
            if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)) {

                // TODO: ensure that queue is bound before first data buffer event arrives

                IOEvents.GenericIOEvent connected =
                    new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, InputReader.this, event.srcTaskID,
                                                event.dstTaskID);
                connected.setChannel(ctx.channel());
                dispatcher.dispatchEvent(connected);
            } else if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                queues.get(channelToQueueIndex.get(ctx.channel())).offer(event);
            } else {
                event.setChannel(ctx.channel());
                dispatcher.dispatchEvent(event);
            }
        }
    }

    // TODO: Make use of flip buffer
    // TODO see FixedLengthFrameDecoder for more error resistant solution
    // TODO use the context lvl buffer provider here instead of i/o lvl
    private class MergeBufferInHandler extends ChannelInboundHandlerAdapter {

        private ByteBuf buf;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            buf = contextLevelAllocator.buffer(bufferSize, bufferSize);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            buf.release();
            buf = null;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg;
            // do not overflow the destination buffer
            buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));

            while (buf.readableBytes() >= bufferSize) {
                ctx.fireChannelRead(buf);
                buf.clear();

                // add rest of buffer to dest
                // sadly this is not true // guaranteed to not overflow, if we set the rcv_buffer size to buffer size
                //assert m.writableBytes() <= bufferSize;
                buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));
            }
            m.release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // TODO: proper exception handling
            cause.printStackTrace();
            ctx.close();
        }
    }

    @Override
    public void connectedChannels(UUID taskID, int gateIndex, int channelIndex) {
        Dummy d = new Dummy();
        d.taskID = taskID;
        d.gateIndex = gateIndex;
        d.channelIndex = channelIndex;
        connected.add(d);
    }

    @Override
    public boolean isConnected(UUID taskID, int gateIndex, int channelIndex) {
        Dummy d = new Dummy();
        d.taskID = taskID;
        d.gateIndex = gateIndex;
        d.channelIndex = channelIndex;
        return connected.contains(d);
    }

    private final Set<Dummy> connected = new HashSet<>();

    public static class Dummy {

        UUID taskID;
        int gateIndex;
        int channelIndex;

        @Override
        public int hashCode() {
            return taskID.hashCode() ^ gateIndex ^ channelIndex;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (hashCode() != obj.hashCode()) {
                return false;
            }
            if (!(obj instanceof Dummy)) {
                return false;
            }

            Dummy other = (Dummy) obj;
            return (taskID == null) ? other.taskID == null : taskID.equals(other.taskID) &&
                                                             gateIndex == other.gateIndex && channelIndex == other.channelIndex;
        }
    }

    public interface ConnectionType {

        ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup);
    }

    public static class LocalConnection implements ConnectionType {

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            b.group(eventLoopGroup)
                .channel(LocalServerChannel.class);
//                .childHandler(new ChannelInitializer<LocalChannel>() {
//                    @Override
//                    public void initChannel(LocalChannel ch) throws Exception {
//                        ch.pipeline()
//                            .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
//                            .addLast(new DataHandler())
//                            .addLast(new EventHandler());
//                    }
//                });

            return b;
        }
    }

    public static class NetworkConnection implements ConnectionType {

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            // TODO: check if its better to use a dedicated boss and worker group instead of one for both
            b.group(eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                    // sets the max. number of pending, not yet fully connected (handshake) channels
                .option(ChannelOption.SO_BACKLOG, 128)
                    // set keep alive, so idle connections are persistent
                .childOption(ChannelOption.SO_KEEPALIVE, true);
//                .childHandler(new ChannelInitializer<SocketChannel>() {
//                    @Override
//                    public void initChannel(SocketChannel ch) throws Exception {
//                        ch.pipeline()
//                            .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
//                            .addLast(new DataHandler())
//                            .addLast(new EventHandler());
//                    }
//                });

            return b;
        }
    }
}
