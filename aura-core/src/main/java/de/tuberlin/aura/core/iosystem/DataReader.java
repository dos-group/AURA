package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.BufferCallback;
import de.tuberlin.aura.core.memory.IAllocator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import de.tuberlin.aura.core.task.common.TaskExecutionUnit;
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


    public final TaskExecutionManager executionManager;

    /**
     * Creates a new Data Reader. There should only be one Data Reader per task manager.
     * 
     * @param dispatcher the dispatcher to which events should be published.
     */
    public DataReader(IEventDispatcher dispatcher, final TaskExecutionManager executionManager) {
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
     * @param type the connection type of the channel to bind.
     * @param address the remote address the channel should be connected to.
     * @param workerGroup the event loop the channel should be associated with.
     * @param <T> the type of channel this connection uses.
     */
    public <T extends Channel> void bind(final InboundConnectionType<T> type, final SocketAddress address, final EventLoopGroup workerGroup) {

        ServerBootstrap bootstrap = type.bootStrap(workerGroup);
        bootstrap.childHandler(type.getPipeline(this));

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
     * TODO: Is the "synchronized" annotation really necessary? Without this annotation access to
     * the channelToQueueIndex rarely causes a NullPointerException. Maybe it is enough to use
     * synchronized maps. However, the performance influence of each solution must be evaluated.
     * <p/>
     * We could execute ~350 topologies one after the other without any problems while using a
     * synchronized version of this method.
     * 
     * @param srcTaskID
     * @param channel
     * @param gateIndex
     * @param channelIndex
     * @param queue
     */
    public synchronized void bindQueue(final UUID srcTaskID,
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

        // LOG.error("input queues: " + inputQueues.size());
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
    // NETTY CHANNEL HANDLER
    // ---------------------------------------------------

    public final class TransferEventHandler extends SimpleChannelInboundHandler<IOEvents.TransferBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.TransferBufferEvent event) {
            try {
                inputQueues.get(channelToQueueIndex.get(ctx.channel())).put(event);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public final class DataEventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

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

    public interface InboundConnectionType<T extends Channel> {

        ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup);

        ChannelInitializer<T> getPipeline(final DataReader dataReader);
    }

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
                      .addLast(new LocalTransferBufferCopyHandler(dataReader.executionManager))
                      .addLast(dataReader.new TransferEventHandler())
                      .addLast(dataReader.new DataEventHandler());
                }
            };
        }
    }

    private static final class LocalTransferBufferCopyHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        private long CALLBACKS = 0;

        private IAllocator allocator;

        private final TaskExecutionManager executionManager;

        private final AtomicInteger pendingCallbacks = new AtomicInteger(0);

        private final LinkedBlockingQueue<PendingEvent> pendingObjects = new LinkedBlockingQueue<>();

        private static class PendingEvent {

            public final long index;

            public final Object event;

            public PendingEvent(final long index, final Object event) {
                this.index = index;
                this.event = event;
            }
        }

        public LocalTransferBufferCopyHandler(TaskExecutionManager executionManager) {
            this.executionManager = executionManager;
        }

        @Override
        public void channelRead0(final ChannelHandlerContext ctx, IOEvents.DataIOEvent msg) throws Exception {
            switch (msg.type) {
                case IOEvents.DataEventType.DATA_EVENT_BUFFER: {
                    // get buffer
                    MemoryView view = allocator.alloc();
                    if (view == null) {
                        view = allocator.alloc(new Callback((IOEvents.TransferBufferEvent) msg, ctx));
                        if (view == null) {
                            if (pendingCallbacks.incrementAndGet() == 1) {
                                ctx.channel().config().setAutoRead(false);
                            }
                            break;
                        }
                    }
                    IOEvents.TransferBufferEvent event = (IOEvents.TransferBufferEvent) msg;
                    System.arraycopy(event.buffer.memory, event.buffer.baseOffset, view.memory, view.baseOffset, event.buffer.size());
                    event.buffer.free();
                    IOEvents.TransferBufferEvent copy = new IOEvents.TransferBufferEvent(event.srcTaskID, event.dstTaskID, view);
                    ctx.fireChannelRead(copy);

                    break;
                }
                default: {
                    if (allocator == null && executionManager != null) {
                        bindAllocator(msg.srcTaskID, msg.dstTaskID);
                    }
                    if (pendingCallbacks.get() >= 1) {
                        // LOG.error("queue");
                        pendingObjects.offer(new PendingEvent(CALLBACKS, msg));
                    } else {
                        ctx.fireChannelRead(msg);
                    }
                    break;
                }
            }
        }

        private class Callback implements BufferCallback {

            private final IOEvents.TransferBufferEvent transferBufferEvent;

            private final ChannelHandlerContext ctx;

            private final long index;

            Callback(final IOEvents.TransferBufferEvent transferBufferEvent, ChannelHandlerContext ctx) {
                this.transferBufferEvent = transferBufferEvent;
                this.ctx = ctx;
                this.index = ++CALLBACKS;
            }

            @Override
            public void bufferReader(final MemoryView buffer) {
                ctx.channel().eventLoop().execute(new Runnable() {

                    @Override
                    public void run() {
                        System.arraycopy(transferBufferEvent.buffer.memory,
                                         transferBufferEvent.buffer.baseOffset,
                                         buffer.memory,
                                         buffer.baseOffset,
                                         transferBufferEvent.buffer.size());
                        transferBufferEvent.buffer.free();
                        IOEvents.TransferBufferEvent copy =
                                new IOEvents.TransferBufferEvent(transferBufferEvent.srcTaskID, transferBufferEvent.dstTaskID, buffer);
                        ctx.fireChannelRead(copy);
                        // LOG.warn("callback run " + index);
                        for (Iterator<PendingEvent> itr = pendingObjects.iterator(); itr.hasNext();) {
                            PendingEvent obj = itr.next();
                            if (obj.index == index) {
                                ctx.fireChannelRead(obj.event);
                                itr.remove();
                            } else {
                                break;
                            }
                        }
                        if (pendingCallbacks.decrementAndGet() == 0) {
                            LOG.error("no pending callbacks + " + pendingObjects.size());
                            ctx.channel().config().setAutoRead(true);
                            ctx.channel().read();
                        }
                    }
                });
            }
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

    public static class NetworkConnection implements InboundConnectionType<SocketChannel> {

        @Override
        public ServerBootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            ServerBootstrap b = new ServerBootstrap();
            // TODO: check if its better to use a dedicated boss and worker group instead of one for
            // both
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
                      .addLast(KryoEventSerializer.LENGTH_FIELD_DECODER())
                      .addLast(KryoEventSerializer.KRYO_OUTBOUND_HANDLER())
                      .addLast(KryoEventSerializer.KRYO_INBOUND_HANDLER(dataReader.executionManager))
                      .addLast(dataReader.new TransferEventHandler())
                      .addLast(dataReader.new DataEventHandler());
                }
            };
        }
    }
}
