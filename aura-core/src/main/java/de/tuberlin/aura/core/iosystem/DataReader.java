package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class DataReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final static Logger LOG = LoggerFactory.getLogger(DataReader.class);

    private final IEventDispatcher dispatcher;

    private final Map<Channel, BufferQueue<IOEvents.DataIOEvent>> channelToQueue = new HashMap<>();

    private final Map<Triple<UUID,Integer,Integer>,Channel> gateKeyToChannel = new HashMap<>();

    public final ITaskExecutionManager executionManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * Creates a new DataReader. A DataReader is always bound to a
     * {@link de.tuberlin.aura.core.task.spi.ITaskExecutionManager}.
     *
     * @param dispatcher the dispatcher used for events
     * @param executionManager the execution manager the data reader is bound to
     */
    public DataReader(final IEventDispatcher dispatcher, final ITaskExecutionManager executionManager) {
        // sanity check.
        if (dispatcher == null)
            throw new IllegalArgumentException("dispatcher == null");
//        if (executionManager == null)
//            throw new IllegalArgumentException("executionManager == null");

        this.dispatcher = dispatcher;

        this.executionManager = executionManager;
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
    public <T extends Channel> void bind(final IInboundConnectionType<T> type, final SocketAddress address, final EventLoopGroup workerGroup) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");
        if (address == null)
            throw new IllegalArgumentException("address == null");
        if (workerGroup == null)
            throw new IllegalArgumentException("workerGroup == null");

        final ServerBootstrap bootstrap = type.bootStrap(workerGroup);
        bootstrap.childHandler(type.getPipeline(this));

        try {
            // Bind and start to accept incoming connections.
            bootstrap.bind(address).addListener(new ChannelFutureListener() {

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
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex, final int channelIndex) {
        return channelToQueue.get(gateKeyToChannel.get(Triple.of(taskID, gateIndex, channelIndex)));
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

        channelToQueue.put(channel, queue);

        gateKeyToChannel.put(Triple.of(srcTaskID, gateIndex, channelIndex), channel);
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
        return gateKeyToChannel.containsKey(Triple.of(taskID, gateIndex, channelIndex));
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
        gateKeyToChannel.get(Triple.of(taskID, gateIndex, channelIndex)).writeAndFlush(event);
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
            channelToQueue.get(ctx.channel()).offer(event);
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
                    IOEvents.DataIOEvent inputConnectedEvent =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, event.srcTaskID, event.dstTaskID);
                    inputConnectedEvent.setPayload(DataReader.this);
                    inputConnectedEvent.setChannel(ctx.channel());
                    dispatcher.dispatchEvent(inputConnectedEvent);
                    break;

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                    channelToQueue.get(ctx.channel()).offer(event);

                    // send acknowledge
                    IOEvents.DataIOEvent acknowledge =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED_ACK, event.dstTaskID, event.srcTaskID);
                    ctx.channel().writeAndFlush(acknowledge);
                    break;

                case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK:
                    channelToQueue.get(ctx.channel()).offer(event);

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
    public interface IInboundConnectionType<T extends Channel> {
        ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup);
        ChannelInitializer<T> getPipeline(final DataReader dataReader);
    }

    /**
     * A local connection for communication between tasks located on the same task manager.
     */
    public static class LocalConnection implements IInboundConnectionType<LocalChannel> {

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
    public static class NetworkConnection implements IInboundConnectionType<SocketChannel> {

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
