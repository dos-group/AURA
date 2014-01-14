package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.iosystem.buffer.NoBufferAllocator;
import de.tuberlin.aura.core.iosystem.buffer.Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


// TODO: let the buffer size be configurable
public class ChannelWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelWriter.class);

    private static final int DEFAULT_BUFFER_SIZE = Util.nextPowerOf2(64 << 10); // 64 KB

    private final InetSocketAddress host;

    private final EventLoopGroup eventLoopGroup;

    private final int bufferSize;

    private volatile boolean shutdown = false;

    private Channel channel;

    private final AtomicBoolean channelWritable = new AtomicBoolean(false);

    private ExecutorService pollThreadExecutor;

    private BufferQueue<IOEvents.DataIOEvent> queue;

    private final ReentrantLock queueLock = new ReentrantLock();
    private final Condition queueReady = queueLock.newCondition();

    // dispatch
    private final IEventDispatcher dispatcher;
    private final UUID srcTaskID;
    private final UUID dstTaskID;
    private Future<IOEvents.DataIOEvent> pollResult;

    public ChannelWriter(final UUID srcTaskID, final UUID dstTaskID, IEventDispatcher dispatcher, InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup) {
        this(srcTaskID, dstTaskID, dispatcher, remoteAddress, eventLoopGroup, DEFAULT_BUFFER_SIZE);
    }

    public ChannelWriter(final UUID srcTaskID, final UUID dstTaskID, IEventDispatcher dispatcher, InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, int bufferSize) {
        this.dispatcher = dispatcher;
        this.srcTaskID = srcTaskID;
        this.dstTaskID = dstTaskID;

        this.host = remoteAddress;
        // Default is 2 times the number of processors available as threads
        this.eventLoopGroup = eventLoopGroup;
        this.bufferSize = bufferSize;

        this.pollThreadExecutor = Executors.newSingleThreadExecutor();

        Bootstrap bs = new Bootstrap();
        bs.group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                        // true, periodically heartbeats from tcp
                .option(ChannelOption.SO_KEEPALIVE, true)
                        // false, means that messages get only sent if the size of the data reached a relevant amount.
                .option(ChannelOption.TCP_NODELAY, false)
                        // size of the system lvl send buffer PER SOCKET
                        // -> buffer size, as we always have only 1 channel per socket in the client case
                .option(ChannelOption.SO_SNDBUF, bufferSize)
                        // the mark the outbound buffer has to reach in order to change the writable state of a channel true
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
                        // the mark the outbound buffer has to reach in order to change the writable state of a channel false
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize)
                        // dummy allocator here, as we do not invoke the allocator in the event loop
                .option(ChannelOption.ALLOCATOR, new NoBufferAllocator())
                        // set the channelWritable spin lock
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new WritableHandler())
                                .addLast(new OutBoundHandler());
                    }
                });

        LOG.debug("Connect to host " + host);

        // on success:
        // the close future is registered
        // the polling thread is started
        bs.connect(host.getAddress(), host.getPort()).addListener(new ConnectListener());

        // connect event handler
        dispatcher.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_QUEUE, new IOClientEventHandler());
    }

    /**
     * Takes buffers from the context queue and writes them to the channel.
     * If the channel is currently not writable, no calls to the channel are made.
     */
    private class Poll implements Callable<IOEvents.DataIOEvent> {

        private final Logger LOG = LoggerFactory.getLogger(Poll.class);

        //private final ByteBufAllocator allocator = new FlipBufferAllocator(bufferSize, 1, true);

        //private ByteBuf flipBuffer;

        @Override
        public IOEvents.DataIOEvent call() throws Exception {
            // wait for queue to be ready
            queueLock.lock();
            try {
                queueReady.await();
            } catch (InterruptedException e) {
                if (shutdown) {
                    LOG.info("Shutdown signal received.", e);
                    return null;
                }
            } finally {
                queueLock.unlock();
            }

            IOEvents.DataIOEvent unfinishedEvent = null;

            while (!shutdown) {
                try {
                    if (ChannelWriter.this.channelWritable.get()) {

                        // TODO: maybe move allocation and transforming in handler...

                        IOEvents.DataIOEvent dataBufferEvent = ChannelWriter.this.queue.take();

                        // deposit in flipbuffer
                        //flipBuffer = allocator.buffer(bufferSize, bufferSize);

                        // TODO: handle release / provide release for data buffer event

                        // TODO: get meta data and then put into buffer
                        //flipBuffer.writeBytes(dataBufferEvent);

                        // in case of an interrupt we can not guarantee that the buffer was sent
                        // -> return the Data event
                        try {
                            //channel.writeAndFlush(flipBuffer).sync();
                            channel.writeAndFlush(dataBufferEvent).sync();
                        } catch (InterruptedException e) {
                            assert shutdown;
                            LOG.warn("write of event " + dataBufferEvent + " was interrupted.", e);
                            unfinishedEvent = dataBufferEvent;
                        }
                    } else {
                        LOG.trace("spinning");
                    }
                } catch (InterruptedException e) {
                    // take interrupted - no DataIOEvent, so no return value
                    // shutdown already set by close method
                    assert shutdown;
                    LOG.info("interrupt while buffer queue was empty.", e);
                }
            }

            LOG.debug("Polling Thread is closing.");

            // take interrupt or normal exit, no data io event left
            return unfinishedEvent;
        }
    }

    /**
     * Shut down the channel writer. If there was an ongoing write which could not be finished,
     * the {@see DataIOEvent} is dispatched.
     */
    public void shutdown() {
        shutdown = true;
        // this should work as we exit the loop on interrupt
        // in case it does not work we have to use a future and wait explicitly
        pollThreadExecutor.shutdownNow();
        LOG.info("CLOSE CHANNEL " + channel);
        channel.disconnect();

        try {
            channel.close().sync();
        } catch (InterruptedException e) {
            LOG.error("Close of channel writer was interrupted", e);
        } finally {
            try {
                IOEvents.DataIOEvent result = pollResult.get();
                if (result != null) {
                    //ChannelWriter.this.dispatchEvent(result);
                }
            } catch (InterruptedException e) {
                LOG.error("Receiving future from poll thread failed. Interrupt.", e);
            } catch (ExecutionException e) {
                LOG.error("Receiving future from poll thread failed. Exception in poll thread", e);
            }
        }
    }

    /**
     * Sets the channel if the connection to the server was successful.
     */
    private class ConnectListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isDone()) {
                if (future.isSuccess()) {

                    ChannelWriter.this.channel = future.channel();
                    LOG.debug("channel successfully connected.");

                    Poll pollThread = new Poll();
                    pollResult = pollThreadExecutor.submit(pollThread);

                    future.channel().writeAndFlush(
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID));
                    final IOEvents.DataIOEvent event =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID);
                    event.setChannel(future.channel());
                    dispatcher.dispatchEvent(event);

                } else if (future.cause() != null) {
                    LOG.error("connection attempt failed: ", future.cause());
                }
            }
        }
    }

    /**
     * Sets the writable flag for this channel.
     */
    private class WritableHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            LOG.trace("channelActive");
            channelWritable.set(ctx.channel().isWritable());
            ctx.fireChannelActive();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            LOG.debug("channelWritabilityChanged: " + (ctx.channel().isWritable() ? "writable" : "blocked"));
            channelWritable.set(ctx.channel().isWritable());
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            super.channelRead(ctx, msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            super.channelReadComplete(ctx);
        }
    }

    private class OutBoundHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            super.read(ctx);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            super.write(ctx, msg, promise);
        }
    }

    /**
     * Attach the queue to the client.
     */
    private final class IOClientEventHandler extends EventHandler {

        @Handle(event = IOEvents.QueueIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_QUEUE)
        private void handleQueueReady(final IOEvents.QueueIOEvent event) {
            if (event.getChannel().equals(channel)) {
                queue = event.queue;
                queueLock.lock();
                try {
                    queueReady.signal();
                } finally {
                    queueLock.unlock();
                }
            }
        }
    }
}
