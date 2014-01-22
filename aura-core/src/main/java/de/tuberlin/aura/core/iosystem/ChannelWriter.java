package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.iosystem.buffer.Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO: let the buffer size be configurable
public class ChannelWriter implements IChannelWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelWriter.class);

    private static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private final InetSocketAddress host;

    private final EventLoopGroup eventLoopGroup;

    private final int bufferSize;

    private volatile boolean shutdown = false;

    private Channel channel;

    private final AtomicBoolean channelWritable = new AtomicBoolean(false);

    private ExecutorService pollThreadExecutor;

    private BufferQueue<IOEvents.DataIOEvent> queue;

    private final CountDownLatch queueReady = new CountDownLatch(1);

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

        if ((bufferSize & (bufferSize - 1)) != 0) {
            // not a power of two
            LOG.warn("The given buffer size is not a power of two.");

            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }

        this.pollThreadExecutor = Executors.newSingleThreadExecutor();

        // connect event handler here, in order to have it ready before connection happens
        dispatcher.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_QUEUE, new IOClientEventHandler());

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
                        //.option(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))
                        // set the channelWritable spin lock
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new WritableHandler())
                                .addLast(new ObjectEncoder());
                    }
                });

        LOG.debug("Connect to host " + host);

        // on success:
        // the close future is registered
        // the polling thread is started
        bs.connect(host.getAddress(), host.getPort()).addListener(new ConnectListener());
    }

    @Override
    public void write(IOEvents.DataIOEvent event) {
        this.queue.offer(event);
    }

    /**
     * Shut down the channel writer. If there was an ongoing write which could not be finished,
     * the {@see DataIOEvent} is dispatched.
     */
    @Override
    public void shutdown() {
        // this should work as we exit the loop on interrupt
        // in case it does not work we have to use a future and wait explicitly

            try {
                IOEvents.DataIOEvent result = pollResult.get();
                if (result != null) {
                    //ChannelWriter.this.dispatchEvent(result);
                }
            } catch (InterruptedException e) {
                LOG.error("Receiving future from poll thread failed. Interrupt.", e);
            } catch (ExecutionException e) {
                LOG.error("Receiving future from poll thread failed. Exception in poll thread", e);
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
            try {
                queueReady.await();
            } catch (InterruptedException e) {
                if (shutdown) {
                    LOG.info("Shutdown signal received, while queue was still not attached.");
                    return null;
                }
            }

            IOEvents.DataIOEvent unfinishedEvent = null;

            while (!shutdown) {
                try {
                    if (ChannelWriter.this.channelWritable.get()) {

                        // TODO: maybe move allocation and transforming in handler...

                        IOEvents.DataIOEvent dataIOEvent = ChannelWriter.this.queue.take();

                        if (dataIOEvent.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                            // shutdown!
                            LOG.info("data source exhausted. shutting down poll thread.");
                            shutdown = true;
                        }

                        // deposit in flipbuffer
                        //flipBuffer = allocator.buffer(bufferSize, bufferSize);

                        // TODO: handle release / provide release for data buffer event

                        // TODO: get meta data and then put into buffer
                        //flipBuffer.writeBytes(dataIOEvent);

                        // in case of an interrupt we can not guarantee that the buffer was sent
                        // -> return the Data event
                        try {
                            //channel.writeAndFlush(flipBuffer).sync();
                            channel.writeAndFlush(dataIOEvent).sync();
                        } catch (InterruptedException e) {
                            assert shutdown;
                            LOG.warn("write of event " + dataIOEvent + " was interrupted.", e);
                            unfinishedEvent = dataIOEvent;
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
                    pollThreadExecutor.shutdown();

                    future.channel().writeAndFlush(
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID));
                    final IOEvents.GenericIOEvent event =
                            new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, ChannelWriter.this, srcTaskID, dstTaskID);
                    event.setChannel(future.channel());
                    dispatcher.dispatchEvent(event);

                } else if (future.cause() != null) {
                    LOG.error("connection attempt failed: ", future.cause());
                    throw new IllegalStateException("connection attempt failed.", future.cause());
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

    /**
     * Attach the queue to the client.
     */
    private final class IOClientEventHandler extends EventHandler {

        @Handle(event = IOEvents.GenericIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_QUEUE)
        private void handleQueueReady(final IOEvents.GenericIOEvent event) {
            if (event.srcTaskID.equals(srcTaskID) && event.dstTaskID.equals(dstTaskID)) {
                queue = (BufferQueue) event.payload;
                LOG.debug("buffer queue attached.");

                queueReady.countDown();
            }
        }
    }
}
