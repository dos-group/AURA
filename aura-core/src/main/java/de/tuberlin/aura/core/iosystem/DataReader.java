package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DataReader {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    public final static Logger LOG = LoggerFactory.getLogger(DataReader.class);

    private final IEventDispatcher dispatcher;

    /**
     * Each queue associated with this data reader is given a unique (per data reader) index.
     */
    private int queueIndex;

    /**
     * (queue index) -> (queue)
     * <p/>
     * There are possibly more channels that write into a single queue. Each {@see
     * de.tuberlin.aura.core.task.gates.InputGate} as exactly one queue.
     */
    private final Map<Integer, BufferQueue<IOEvents.DataIOEvent>> inputQueues;

    /**
     * (channel) -> (queue index)
     * <p/>
     * Maps the channel to the index of the queue (for map {@see inputQueues}).
     */
    private final Map<Channel, Integer> channelToQueueIndex;

    /**
     * (task id, gate index) -> (queue index)
     * <p/>
     * Maps the gate of a specific task to the index of the queue (for map {@see inputQueues}).
     */
    private final Map<Pair<UUID, Integer>, Integer> gateToQueueIndex;

    /**
     * (task id, gate index) -> { (channel index) -> (channel) }
     * <p/>
     * Maps the gate of a specific task to its connected channels. The connected channels are
     * identified by their channel index.
     */
    private final Map<Pair<UUID, Integer>, Map<Integer, Channel>> connectedChannels;

    /**
     * Creates a new Data Reader. There should only be one Data Reader per task manager.
     *
     * @param dispatcher the dispatcher to which events should be published.
     */
    public DataReader(IEventDispatcher dispatcher) {
        this.dispatcher = dispatcher;

        inputQueues = new HashMap<>();
        channelToQueueIndex = new HashMap<>();
        gateToQueueIndex = new HashMap<>();
        connectedChannels = new HashMap<>();

        queueIndex = 0;
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     * Binds a new channel to the data reader.
     * <p/>
     * Notice: the reader does not shut down the worker group. This is in the callers
     * responsibility.
     *
     * @param type        the connection type of the channel to bind.
     * @param address     the remote address the channel should be connected to.
     * @param workerGroup the event loop the channel should be associated with.
     * @param <T>         the type of channel this connection uses.
     */
    public <T extends Channel> void bind(final ConnectionType<T> type, final SocketAddress address, final EventLoopGroup workerGroup) {

        ServerBootstrap bootstrap = type.bootStrap(workerGroup);
        bootstrap.childHandler(new ChannelInitializer<T>() {

            @Override
            public void initChannel(T ch) throws Exception {
                ch.pipeline()
                        .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
                        .addLast(new DataHandler())
                        .addLast(new EventHandler())
                        .addLast(new ObjectEncoder());
            }
        });

        try {
            // Bind and start to accept incoming connections.
            ChannelFuture f = bootstrap.bind(address).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        LOG.info("network server bound to address " + address);
                    } else {
                        LOG.error("bound attempt failed.", future.cause());
                        throw new IllegalStateException("could not start netty network server", future.cause());
                    }
                }
            }).sync();

            // TODO: add explicit close hook?!

        } catch (InterruptedException e) {
            LOG.error("failed to initialize network server", e);
            throw new IllegalStateException("failed to initialize network server", e);
        }
    }

    /**
     * Returns the queue which is assigned to the gate of the task.
     *
     * @param taskID    the task id which the gate belongs to.
     * @param gateIndex the gate index.
     * @return the queue assigned to the gate, or null if no queue is assigned.
     */
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex) {
        if (!gateToQueueIndex.containsKey(new Pair<UUID, Integer>(taskID, gateIndex))) {
            return null;
        }
        return inputQueues.get(gateToQueueIndex.get(new Pair<UUID, Integer>(taskID, gateIndex)));
    }


    public void bindQueue(final UUID srcTaskID,
                          final Channel channel,
                          final int gateIndex,
                          final int channelIndex,
                          final BufferQueue<IOEvents.DataIOEvent> queue) {

        Pair<UUID, Integer> index = new Pair<>(srcTaskID, gateIndex);
        final int queueIndex;
        if (gateToQueueIndex.containsKey(index)) {
            queueIndex = gateToQueueIndex.get(index);
        } else {
            queueIndex = newQueueIndex();
            inputQueues.put(queueIndex, queue);
            gateToQueueIndex.put(index, queueIndex);
        }
        channelToQueueIndex.put(channel, queueIndex);

        final Map<Integer, Channel> channels;
        if (!connectedChannels.containsKey(index)) {
            channels = new HashMap<>();
        } else {
            channels = connectedChannels.get(index);
        }
        channels.put(channelIndex, channel);
        connectedChannels.put(index, channels);
    }

    private Integer newQueueIndex() {
        return queueIndex++;
    }

    public boolean isConnected(UUID contextID, int gateIndex, int channelIndex) {
        Pair<UUID, Integer> index = new Pair<>(contextID, gateIndex);
        return connectedChannels.containsKey(index) && connectedChannels.get(index).containsKey(channelIndex);

    }

    public void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        Pair<UUID, Integer> index = new Pair<>(taskID, gateIndex);
        connectedChannels.get(index).get(channelIndex).writeAndFlush(event);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class DataHandler extends SimpleChannelInboundHandler<IOEvents.DataBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataBufferEvent event) {
            try {

                inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private final class EventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataIOEvent event) throws Exception {

            switch (event.type) {
                case IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED:
                    // TODO: ensure that queue is bound before first data buffer event arrives

                    IOEvents.GenericIOEvent connected =
                            new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                    DataReader.this,
                                    event.srcTaskID,
                                    event.dstTaskID);
                    connected.setChannel(ctx.channel());
                    dispatcher.dispatchEvent(connected);
                    break;

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                    inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);
                    break;

                case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK:
                    inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);
                    break;

                default:
                    event.setChannel(ctx.channel());
                    dispatcher.dispatchEvent(event);
                    break;
            }
        }
    }

    // ---------------------------------------------------
    // Inner Classes (Strategies).
    // ---------------------------------------------------

    public interface ConnectionType<T> {

        ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup);
    }

    public static class LocalConnection implements ConnectionType<LocalChannel> {

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            b.group(eventLoopGroup).channel(LocalServerChannel.class);

            return b;
        }
    }

    public static class NetworkConnection implements ConnectionType<SocketChannel> {

        private final int bufferSize;

        public NetworkConnection(int bufferSize) {
            if ((bufferSize & (bufferSize - 1)) != 0) {
                // not a power of two
                LOG.warn("The given buffer size is not a power of two.");

                this.bufferSize = Util.nextPowerOf2(bufferSize);
            } else {
                this.bufferSize = bufferSize;
            }
        }

        public NetworkConnection() {
            this(DEFAULT_BUFFER_SIZE);
        }

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            // TODO: check if its better to use a dedicated boss and worker group instead of one for
            // both
            b.group(eventLoopGroup).channel(NioServerSocketChannel.class)
                    // sets the max. number of pending, not yet fully connected (handshake) channels
                    .option(ChannelOption.SO_BACKLOG, 128)

                    .option(ChannelOption.SO_RCVBUF, bufferSize)
                            // set keep alive, so idle connections are persistent
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            return b;
        }
    }
}
