package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO: let the buffer size be configurable
public class DataWriter {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    // dispatch information
    private final IEventDispatcher dispatcher;

    private final EventLoopGroup workerGroup;

    private ExecutorService pollThreadExecutor;

    public DataWriter(IEventDispatcher dispatcher, final EventLoopGroup workerGroup) {
        this(dispatcher, workerGroup, DEFAULT_BUFFER_SIZE);
    }

    public DataWriter(IEventDispatcher dispatcher, final EventLoopGroup workerGroup, int bufferSize) {
        this.dispatcher = dispatcher;
        this.workerGroup = workerGroup;

        this.pollThreadExecutor = Executors.newCachedThreadPool();
    }

    public <T extends Channel> ChannelWriter<T> bind(final UUID srcTaskID,
                                                     final UUID dstTaskID,
                                                     final OutgoingConnectionType<T> type,
                                                     final SocketAddress address) {
        return new ChannelWriter<>(type, srcTaskID, dstTaskID, address);
    }

    public class ChannelWriter<T extends Channel> {

        private final AtomicBoolean channelWritable = new AtomicBoolean(false);

        private final CountDownLatch queueReady = new CountDownLatch(1);

        private final CountDownLatch pollFinished = new CountDownLatch(1);

        private final UUID srcID;

        private final UUID dstID;

        private Channel channel;

        private volatile boolean shutdown = false;

        private BufferQueue<IOEvents.DataIOEvent> queue;

        private Future<Void> pollResult;

        private IOEvents.DataIOEvent interruptedEvent;

        public ChannelWriter(final OutgoingConnectionType<T> type, final UUID srcID, final UUID dstID, final SocketAddress address) {
            this.srcID = srcID;
            this.dstID = dstID;

            Bootstrap bootstrap = type.bootStrap(workerGroup);
            bootstrap.handler(new ChannelInitializer<T>() {

                @Override
                protected void initChannel(T ch) throws Exception {
                    ch.pipeline().addLast(new WritableHandler()).addLast(new ObjectEncoder());
                }
            });

            // on success:
            // the close future is registered
            // the polling thread is started
            bootstrap.connect(address).addListener(new ConnectListener());
        }

        /**
         * Sets the channel if the connection to the server was successful.
         */
        private class ConnectListener implements ChannelFutureListener {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isDone()) {
                    if (future.isSuccess()) {

                        channel = future.channel();
                        LOG.debug("channel successfully connected.");

                        Poll pollThread = new Poll();
                        pollResult = pollThreadExecutor.submit(pollThread);

                        future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                srcID,
                                dstID));
                        final IOEvents.GenericIOEvent event =
                                new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
                                        ChannelWriter.this,
                                        srcID,
                                        dstID);
                        event.setChannel(future.channel());
                        dispatcher.dispatchEvent(event);

                    } else if (future.cause() != null) {
                        LOG.error("connection attempt failed: ", future.cause());
                        throw new IllegalStateException("connection attempt failed.", future.cause());
                    }
                }
            }
        }

        public void write(IOEvents.DataIOEvent event) {
            this.queue.offer(event);
        }

        /**
         * Shut down the channel writer. If there was an ongoing write which could not be finished,
         * the {@see DataIOEvent} is dispatched.
         */
        public void shutdown(boolean awaitExhaustion) {
            // this should work as we exit the loop on interrupt
            // in case it does not work we have to use a future and wait explicitly

            // force interrupt
            if (!awaitExhaustion) {
                // stops send, even if events left in the queue
                shutdown = true;
            }

            // even if we shutdown gracefully, we have to send an interrupt
            // otherwise the thread would never return, if the queue is already empty and the poll
            // thread is blocked in the take method.
            pollResult.cancel(true);

            try {
                // we can't use the return value of result cause we have to interrupt the thread
                // therefore we need the latch and the field
                pollFinished.await();
                if (interruptedEvent != null) {
                    // dispatcher.dispatchEvent(interruptedEvent);
                }
            } catch (InterruptedException e) {
                LOG.error("Receiving future from poll thread failed. Interrupt.", e);
            } finally {
                LOG.info("CLOSE CHANNEL " + channel);
                channel.disconnect();

                try {
                    channel.close().sync();
                } catch (InterruptedException e) {
                    LOG.error("Close of channel writer was interrupted", e);
                }
            }
        }

        public void setOutputQueue(BufferQueue<IOEvents.DataIOEvent> queue) {
            this.queue = queue;
            LOG.debug("buffer queue attached.");
            queueReady.countDown();
        }

        /**
         * Takes buffers from the context queue and writes them to the channel. If the channel is
         * currently not writable, no calls to the channel are made.
         */
        private class Poll implements Callable<Void> {

            private final Logger LOG = LoggerFactory.getLogger(Poll.class);

            @Override
            public Void call() throws Exception {
                try {
                    queueReady.await();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        LOG.info("Shutdown signal received, while queue was still not attached.");
                        return null;
                    }
                }

                LOG.debug("WHILE");

                while (!shutdown) {
                    try {
                        if (channelWritable.get()) {

                            IOEvents.DataIOEvent dataIOEvent = queue.take();

                            if (dataIOEvent.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                                LOG.debug("data source exhausted. shutting down poll thread.");
                                shutdown = true;
                            }

                            // try {
                            channel.writeAndFlush(dataIOEvent).syncUninterruptibly();
                            // } catch (InterruptedException e) {
                            // // 1. interrupt while writing
                            // // 2. interrupt while the queue already acquired the lock in the take
                            // method
                            // // -> take is not canceled, handle here
                            // if (!shutdown) {
                            // LOG.error("unexpected interrupt while sending event.", e);
                            // shutdown = true;
                            // }
                            //
                            // interruptedEvent = dataIOEvent;
                            // }
                        } else {
                            LOG.debug("spinning");
                        }
                    } catch (InterruptedException e) {
                        // interrupted during take command, 2. scenarios
                        // 1. interrupt while queue was empty -> event == null, queue == empty
                        // 2. interrupt before the queue acquired the lock -> event == null, queue
                        // == not empty

                        // if shutdown is NOT set, no normal termination, soo log exception?
                        // if (!shutdown) {
                        // LOG.error("unexpected interrupt while buffer queue was empty.", e);
                        // shutdown = true;
                        // }
                    }
                }

                LOG.debug("Polling Thread is closing.");

                pollFinished.countDown();
                return null;
            }
        }

        /**
         * Sets the writable flag for this channel.
         */
        private class WritableHandler extends ChannelInboundHandlerAdapter {

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                // LOG.trace("channelActive");
                channelWritable.set(ctx.channel().isWritable());
                ctx.fireChannelActive();
            }

            @Override
            public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
                // LOG.debug("channelWritabilityChanged: " + (ctx.channel().isWritable() ?
                // "writable" : "blocked"));
                channelWritable.set(ctx.channel().isWritable());
                ctx.fireChannelWritabilityChanged();
            }
        }
    }

    public interface OutgoingConnectionType<T> {

        Bootstrap bootStrap(EventLoopGroup eventLoopGroup);
    }

    public static class LocalConnection implements OutgoingConnectionType<LocalChannel> {

        private final int bufferSize;

        public LocalConnection() {
            this(DEFAULT_BUFFER_SIZE);
        }

        public LocalConnection(int bufferSize) {
            if ((bufferSize & (bufferSize - 1)) != 0) {
                // not a power of two
                LOG.warn("The given buffer size is not a power of two.");

                this.bufferSize = Util.nextPowerOf2(bufferSize);
            } else {
                this.bufferSize = bufferSize;
            }
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(LocalChannel.class)
                    // the mark the outbound buffer has to reach in order to change the writable state of a
                    // channel true
                    .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
                            // the mark the outbound buffer has to reach in order to change the writable state of
                            // a channel false
                    .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);
            // dummy allocator here, as we do not invoke the allocator in the event loop
            // .option(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))
            // set the channelWritable spin lock

            return bs;
        }
    }

    public static class NetworkConnection implements OutgoingConnectionType<SocketChannel> {

        private final int bufferSize;

        public NetworkConnection() {
            this(DEFAULT_BUFFER_SIZE);
        }

        public NetworkConnection(int bufferSize) {
            if ((bufferSize & (bufferSize - 1)) != 0) {
                // not a power of two
                LOG.warn("The given buffer size is not a power of two.");

                this.bufferSize = Util.nextPowerOf2(bufferSize);
            } else {
                this.bufferSize = bufferSize;
            }
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(NioSocketChannel.class)
                    // true, periodically heartbeats from tcp
                    .option(ChannelOption.SO_KEEPALIVE, true)
                            // false, means that messages get only sent if the size of the data reached a relevant
                            // amount.
                    .option(ChannelOption.TCP_NODELAY, false)
                            // size of the system lvl send buffer PER SOCKET
                            // -> buffer size, as we always have only 1 channel per socket in the client case
                    .option(ChannelOption.SO_SNDBUF, bufferSize)
                            // the mark the outbound buffer has to reach in order to change the writable state of
                            // a channel true
                    .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
                            // the mark the outbound buffer has to reach in order to change the writable state of
                            // a channel false
                    .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);
            // dummy allocator here, as we do not invoke the allocator in the event loop
            // .option(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))
            // set the channelWritable spin lock

            return bs;
        }
    }
}
