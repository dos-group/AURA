package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.iosystem.buffer.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;


// TODO: let the buffer size be configurable
public abstract class AbstractWriter implements IChannelWriter {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWriter.class);

    protected final int bufferSize;
    private final AtomicBoolean channelWritable = new AtomicBoolean(false);
    private final CountDownLatch queueReady = new CountDownLatch(1);

    // dispatch information
    private final IEventDispatcher dispatcher;
    private final UUID srcTaskID;
    private final UUID dstTaskID;
    protected final SocketAddress remoteAddress;
    protected final EventLoopGroup eventLoopGroup;

    private Channel channel;

    private volatile boolean shutdown = false;

    private ExecutorService pollThreadExecutor;
    private BufferQueue<IOEvents.DataIOEvent> queue;
    private Future<IOEvents.DataIOEvent> pollResult;

    public AbstractWriter(final UUID srcTaskID, final UUID dstTaskID, SocketAddress remoteAddress, EventLoopGroup eventLoopGroup, IEventDispatcher dispatcher) {
        this(srcTaskID, dstTaskID, dispatcher, remoteAddress, eventLoopGroup, DEFAULT_BUFFER_SIZE);
    }

    public AbstractWriter(final UUID srcTaskID, final UUID dstTaskID, IEventDispatcher dispatcher, SocketAddress remoteAddress, EventLoopGroup eventLoopGroup, int bufferSize) {
        this.srcTaskID = srcTaskID;
        this.dstTaskID = dstTaskID;
        this.dispatcher = dispatcher;
        this.remoteAddress = remoteAddress;
        this.eventLoopGroup = eventLoopGroup;

        if ((bufferSize & (bufferSize - 1)) != 0) {
            // not a power of two
            LOG.warn("The given buffer size is not a power of two.");

            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }

        this.pollThreadExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public abstract void connect();

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

        // force interrupt
        shutdown = true;
        pollThreadExecutor.shutdownNow();

        try {
            IOEvents.DataIOEvent result = pollResult.get();
            if (result != null) {
                //NetworkChannelWriter.this.dispatchEvent(result);
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

    @Override
    public void setOutputQueue(BufferQueue<IOEvents.DataIOEvent> queue) {
        this.queue = queue;
        LOG.debug("buffer queue attached.");
        queueReady.countDown();
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
                    if (channelWritable.get()) {

                        IOEvents.DataIOEvent dataIOEvent = queue.take();

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

                    // if shutdown is NOT set, no normal termination, soo log exception?
                    if (!shutdown) {
                        LOG.error("interrupt while buffer queue was empty.", e);
                        shutdown = true;
                    }
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
    protected class ConnectListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isDone()) {
                if (future.isSuccess()) {

                    channel = future.channel();
                    LOG.debug("channel successfully connected.");

                    Poll pollThread = new Poll();
                    pollResult = pollThreadExecutor.submit(pollThread);
                    pollThreadExecutor.shutdown();

                    future.channel().writeAndFlush(
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID));
                    final IOEvents.GenericIOEvent event =
                            new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, AbstractWriter.this, srcTaskID, dstTaskID);
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
    protected class WritableHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //LOG.trace("channelActive");
            channelWritable.set(ctx.channel().isWritable());
            ctx.fireChannelActive();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            //LOG.debug("channelWritabilityChanged: " + (ctx.channel().isWritable() ? "writable" : "blocked"));
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
}
