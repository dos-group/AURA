package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class DataReader {

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
     * The {@link de.tuberlin.aura.core.task.spi.ITaskExecutionManager} the data reader is bound.
     */
    public final ITaskExecutionManager executionManager;

    /**
     * Creates a new DataReader. A DataReader is always bound to a
     * {@link de.tuberlin.aura.core.task.spi.ITaskExecutionManager}.
     * 
     * @param dispatcher the dispatcher used for events
     * @param executionManager the execution manager the data reader is bound to
     */
    public DataReader(IEventDispatcher dispatcher, final ITaskExecutionManager executionManager) {
        this.dispatcher = dispatcher;
        this.executionManager = executionManager;
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
     * @param type the connection type of the channel to bind
     * @param address the remote address the channel should be connected to
     * @param workerGroup the event loop the channel should be associated with
     * @param <T> the type of channel this connection uses
     */
    public <T extends Channel> void bind(final InboundConnectionType<T> type, final SocketAddress address, final EventLoopGroup workerGroup) {

        final ServerBootstrap bootstrap = type.bootStrap(workerGroup);
        bootstrap.childHandler(type.getPipeline(this));

        try {
            // Bind and start to accept incoming connections.
            final ChannelFuture f = bootstrap.bind(address).addListener(new ChannelFutureListener() {

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
     * @param taskID the task id which the gate belongs to.
     * @param gateIndex the gate index.
     * @return the queue assigned to the gate, or null if no queue is assigned.
     */
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex) {
        if (!gateToQueueIndex.containsKey(new Pair<>(taskID, gateIndex))) {
            return null;
        }
        return inputQueues.get(gateToQueueIndex.get(new Pair<>(taskID, gateIndex)));
    }

    /**
     * Binds a inbound queue to this data reader.
     * 
     * @param srcTaskID the UUID of the task
     * @param channel the channel the queue belongs to
     * @param gateIndex the gate the queue belongs to
     * @param channelIndex the channel index the queue belongs to
     * @param queue the queue
     */
    public synchronized void bindQueue(final UUID srcTaskID,
                                       final Channel channel,
                                       final int gateIndex,
                                       final int channelIndex,
                                       final BufferQueue<IOEvents.DataIOEvent> queue) {

        final Pair<UUID, Integer> index = new Pair<>(srcTaskID, gateIndex);
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

    /**
     * Returns true if the channel is already bound to this data reader.
     * 
     * @param taskID the UUID of the task
     * @param gateIndex the gate index
     * @param channelIndex the channel index
     * @return true if the channel is already bound to this data reader, false otherwise
     */
    public boolean isConnected(final UUID taskID, final int gateIndex, final int channelIndex) {
        final Pair<UUID, Integer> index = new Pair<>(taskID, gateIndex);
        return connectedChannels.containsKey(index) && connectedChannels.get(index).containsKey(channelIndex);
    }

    /**
     * Writes a {@link de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent} to the channel bound
     * unique identifer triple (taskID, gateIndex, channelIndex).
     * 
     * @param taskID the task id
     * @param gateIndex the gate index
     * @param channelIndex the channel index
     * @param event the event that is written to the channel
     */
    public void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        final Pair<UUID, Integer> index = new Pair<>(taskID, gateIndex);
        connectedChannels.get(index).get(channelIndex).writeAndFlush(event);
    }

    // ---------------------------------------------------
    // NETTY CHANNEL HANDLER
    // ---------------------------------------------------

    /**
     * Handles {@link de.tuberlin.aura.core.iosystem.IOEvents.TransferBufferEvent}.
     */
    public final class TransferBufferEventHandler extends SimpleChannelInboundHandler<IOEvents.TransferBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.TransferBufferEvent event) {
            try {
                inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);
            } catch (InterruptedException e) {
                LOG.error("put on input queue was interrupted.", e);
            }
        }
    }

    /**
     * Handles all {@link de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent} apart from
     * {@link de.tuberlin.aura.core.iosystem.IOEvents.TransferBufferEvent}.
     */
    public final class DataIOEventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataIOEvent event) throws Exception {
            switch (event.type) {
                case IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED:
                    // TODO: ensure that queue is bound before first data buffer event arrives
                    IOEvents.DataIOEvent connected =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, event.srcTaskID, event.dstTaskID);
                    connected.setPayload(DataReader.this);
                    connected.setChannel(ctx.channel());

                    dispatcher.dispatchEvent(connected);
                    break;

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                    inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);
                    // send acknowledge
                    IOEvents.DataIOEvent acknowledge =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED_ACK, event.dstTaskID, event.srcTaskID);
                    ctx.channel().writeAndFlush(acknowledge);
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

    /**
     * Specifies a inbound connection type.
     * 
     * Implementing classes are responsible to provide a {@link io.netty.bootstrap.ServerBootstrap}
     * and the handler pipeline for netty.
     * 
     * @param <T> the channel type used by the connection
     */
    public interface InboundConnectionType<T extends Channel> {

        ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup);

        ChannelInitializer<T> getPipeline(final DataReader dataReader);
    }

    /**
     * A local connection for communication between tasks located on the same task manager.
     */
    public static class LocalConnection implements InboundConnectionType<LocalChannel> {

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            b.group(eventLoopGroup).channel(LocalServerChannel.class);

            return b;
        }

        @Override
        public ChannelInitializer<LocalChannel> getPipeline(final DataReader dataReader) {
            return new ChannelInitializer<LocalChannel>() {

                @Override
                public void initChannel(LocalChannel ch) throws Exception {
                    ch.pipeline()
                      .addLast(new SerializationHandler.LocalTransferBufferCopyHandler(dataReader.executionManager))
                      .addLast(dataReader.new TransferBufferEventHandler())
                      .addLast(dataReader.new DataIOEventHandler());
                }
            };
        }
    }

    /**
     * A tcp connection for communication between network connected tasks.
     */
    public static class NetworkConnection implements InboundConnectionType<SocketChannel> {

        // TODO: check if its better to use a dedicated boss and worker group
        // TODO: check parameter values, like SO_RCVBUF

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            b.group(eventLoopGroup).channel(NioServerSocketChannel.class)
            // sets the max. number of pending, not yet fully connected (handshake) channels
             .option(ChannelOption.SO_BACKLOG, 1024)
             // .option(ChannelOption.SO_RCVBUF, IOConfig.NETTY_RECEIVE_BUFFER_SIZE)
             // set keep alive, so idle connections are persistent
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            return b;
        }

        @Override
        public ChannelInitializer<SocketChannel> getPipeline(final DataReader dataReader) {
            return new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                      .addLast(SerializationHandler.LENGTH_FIELD_DECODER())
                      .addLast(SerializationHandler.KRYO_OUTBOUND_HANDLER())
                      .addLast(SerializationHandler.KRYO_INBOUND_HANDLER(dataReader.executionManager))
                      .addLast(dataReader.new TransferBufferEventHandler())
                      .addLast(dataReader.new DataIOEventHandler());
                }
            };
        }
    }
}
