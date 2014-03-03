package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;


// TODO: let the bufferQueue size be configurable
public class DataWriter {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    private final ReentrantLock drainLock = new ReentrantLock();

    private final Condition drainCond = drainLock.newCondition();

    // dispatch information
    private final IEventDispatcher dispatcher;

    private final EventLoopGroup workerGroup;

    private ExecutorService pollThreadExecutor;

    public DataWriter(IEventDispatcher dispatcher, final EventLoopGroup workerGroup) {
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

        private BufferQueue<IOEvents.DataIOEvent> transferQueue;

        private BufferQueue<IOEvents.DataIOEvent> bufferQueue;

        private Future<Void> pollResult;

        private IOEvents.DataIOEvent interruptedEvent;

        private AtomicBoolean gateOpen = new AtomicBoolean(false);

        public ChannelWriter(final OutgoingConnectionType<T> type, final UUID srcID, final UUID dstID, final SocketAddress address) {
            this.srcID = srcID;
            this.dstID = dstID;

            Bootstrap bootstrap = type.bootStrap(workerGroup);
            bootstrap.handler(new ChannelInitializer<T>() {

                @Override
                protected void initChannel(T ch) throws Exception {
                    ch.pipeline()
                      .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
                      .addLast(new OpenCloseGateHandler())
                      .addLast(new WritableHandler())
                      .addLast(new ObjectEncoder());
                }
            });

            // on success:
            // the close future is registered
            // the polling thread is started
            bootstrap.connect(address).addListener(new ConnectListener());
        }

        public void setBufferQueue(BufferQueue<IOEvents.DataIOEvent> bufferQueue) {
            this.bufferQueue = bufferQueue;
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

        /**
         * Writes the event to the channel.
         * 
         * If the the gate the channel is connected to is not open yet, the events are buffered in a
         * transferQueue. If this intermediate transferQueue is full, this method blocks until the
         * gate is opened.
         * 
         * @param event
         */
        public void write(IOEvents.DataIOEvent event) {

            if (gateOpen.get()) {
                try {
                    this.transferQueue.put(event);

                    // if it was blocked, and we have elements in the bufferQueue, we first have to
                    // drain
                    // the bufferQueue to the transferQueue before we allow further
                    // writes to the transferQueue to achieve in order.

                } catch (InterruptedException e) {
                    LOG.error("Write of event " + event + " was interrupted.", e);
                }
            } else {
          // add into bufferQueue
                final Lock lock = DataWriter.this.drainLock;
                lock.lock();
                try {
                    // TODO: handle spurious wakeup!
                    if (!this.bufferQueue.offer(event)) {
                        drainCond.await();
                        // write elements which was not enqueued into the bufferQueue to outgoing
                        // transferQueue
                        this.transferQueue.put(event);
                    }

          // if it was blocked, drain to transferQueue
                    // done by handler for the open gate event

                } catch (InterruptedException e) {
                    LOG.error("Write of event " + event + " was interrupted (buffer handle).", e);
                } finally {
                    lock.unlock();
                }
            }

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
                // stops send, even if events left in the transferQueue
                shutdown = true;
            }

            // even if we shutdown gracefully, we have to send an interrupt
            // otherwise the thread would never return, if the transferQueue is already empty and the poll
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
            this.transferQueue = queue;
        LOG.debug("Buffer queue attached.");
            queueReady.countDown();
        }

        /**
         * Takes buffers from the context transferQueue and writes them to the channel. If the
         * channel is currently not writable, no calls to the channel are made.
         */
        private class Poll implements Callable<Void> {

            private final Logger LOG = LoggerFactory.getLogger(Poll.class);

            @Override
            public Void call() throws Exception {
                try {
                    queueReady.await();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        LOG.info("Shutdown signal received, while transferQueue was still not attached.");
                        return null;
                    }
                }

                while (!shutdown) {
                    try {
                        if (channelWritable.get()) {

                            IOEvents.DataIOEvent dataIOEvent = transferQueue.take();

                            if (dataIOEvent instanceof IOEvents.DataBufferEvent) {
                                int received = ByteBuffer.wrap(((IOEvents.DataBufferEvent) dataIOEvent).data).getInt();
                                LOG.error("sending :" + received);
                            }

                            if (dataIOEvent.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                                LOG.debug("data source exhausted. shutting down poll thread.");
                                shutdown = true;
                            }

                            channel.writeAndFlush(dataIOEvent).syncUninterruptibly();
                        } else {
     LOG.trace("Channel not writable.");
                        }
                    } catch (InterruptedException e) {
                        // interrupted during take command, 2. scenarios
                        // 1. interrupt while transferQueue was empty -> event == null,
                        // transferQueue == empty
                        // 2. interrupt before the transferQueue acquired the lock -> event == null,
                        // transferQueue == not empty

                        // either the thread is forced to shut down -> shutdown == true
                        // or gracefully, send events in transferQueue before shutdown -> shutdown == false
                    }
                }

                LOG.debug("Polling Thread is closing.");

                // signal shutdown method
                pollFinished.countDown();
                return null;
            }
        }

        private class OpenCloseGateHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, IOEvents.DataIOEvent gateEvent) throws Exception {

                switch (gateEvent.type) {
                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN:
                        LOG.info("RECEIVED GATE OPEN EVENT");

               // bufferQueue may have more capacity than transferQueue
                        // so we have to wait until elements are sent over net to drain the rest

                        // TODO: blocking, so move away from I/O loop!!!

                        final Lock lock = DataWriter.this.drainLock;
                        lock.lock();
                        try {
                            while (!bufferQueue.isEmpty()) {
                           bufferQueue.drainTo(transferQueue);
                            }
                            drainCond.signalAll();
                        } finally {
                            lock.unlock();
                        }

                        gateOpen.set(true);

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);
                        // dispatch event to output gate

                        break;

                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE:
                        LOG.info("RECEIVED GATE CLOSE EVENT");

                 // disable write to output transferQueue
                        gateOpen.set(false);

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);

                        // as the gate is closed, now events could be enqueued at this point
                        IOEvents.DataIOEvent closedGate =
                                new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_FINISHED, srcID, dstID);
                        transferQueue.offer(closedGate);

                        break;

                    default:
                        LOG.error("RECEIVED UNKNOWN EVENT TYPE: " + gateEvent.type);
                        break;
                }
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

    public static abstract class AbstractConnection<T> implements OutgoingConnectionType<T> {

        protected final int bufferSize;

        public AbstractConnection(int bufferSize) {
            if ((bufferSize & (bufferSize - 1)) != 0) {
                // not a power of two
                LOG.warn("The given buffer size is not a power of two.");

                this.bufferSize = Util.nextPowerOf2(bufferSize);
            } else {
                this.bufferSize = bufferSize;
            }
        }

        public AbstractConnection() {
            this(DEFAULT_BUFFER_SIZE);
        }
    }

    public static class LocalConnection extends AbstractConnection<LocalChannel> {

        public LocalConnection() {
            super(DEFAULT_BUFFER_SIZE);
        }

        public LocalConnection(int bufferSize) {
            super(bufferSize);
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(LocalChannel.class)
            // the mark the outbound bufferQueue has to reach in order to change the writable state of a
            // channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order to change the writable state of
              // a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }

    public static class NetworkConnection extends AbstractConnection<SocketChannel> {

        public NetworkConnection() {
            super(DEFAULT_BUFFER_SIZE);
        }

        public NetworkConnection(int bufferSize) {
            super(bufferSize);
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
              // size of the system lvl send bufferQueue PER SOCKET
              // -> bufferQueue size, as we always have only 1 channel per socket in the client case
              .option(ChannelOption.SO_SNDBUF, bufferSize)
              // the mark the outbound bufferQueue has to reach in order to change the writable state of
              // a channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order to change the writable state of
              // a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }
}
