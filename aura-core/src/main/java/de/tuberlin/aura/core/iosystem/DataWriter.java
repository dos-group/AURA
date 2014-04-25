package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.ResettableCountDownLatch;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.statistic.MeasurementType;
import de.tuberlin.aura.core.statistic.NumberMeasurement;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;


// TODO: let the bufferQueue size be configurable
public class DataWriter {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    private final IEventDispatcher dispatcher;

    private ExecutorService pollThreadExecutor;

    public DataWriter(IEventDispatcher dispatcher) {

        this.dispatcher = dispatcher;

        this.pollThreadExecutor = Executors.newCachedThreadPool();
    }

    public <T extends Channel> ChannelWriter<T> bind(final UUID srcTaskID,
                                                     final UUID dstTaskID,
                                                     final OutgoingConnectionType<T> type,
                                                     final SocketAddress address,
                                                     final EventLoopGroup eventLoopGroup,
                                                     final MemoryManager.Allocator allocator) {

        return new ChannelWriter<>(type, srcTaskID, dstTaskID, address, eventLoopGroup, allocator);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class ChannelWriter<T extends Channel> {

        // connection

        private final UUID srcID;

        private final UUID dstID;

        private Channel channel;

        private final EventLoopGroup eventLoopGroup;

        // poll thread

        private volatile boolean shutdown = false;

        private final CountDownLatch pollFinished = new CountDownLatch(1);

        private Future<?> pollResult;

        private final CountDownLatch queueReady = new CountDownLatch(1);

        private final AtomicBoolean channelWritable = new AtomicBoolean(false);

        private BufferQueue<IOEvents.DataIOEvent> transferQueue;

        // gate semantics

        private final ResettableCountDownLatch awaitGateOpenLatch;

        private AtomicBoolean gateOpen = new AtomicBoolean(false);

        public ChannelWriter(final OutgoingConnectionType<T> type,
                             final UUID srcID,
                             final UUID dstID,
                             final SocketAddress address,
                             final EventLoopGroup eventLoopGroup,
                             final MemoryManager.Allocator allocator) {

            this.srcID = srcID;

            this.dstID = dstID;

            this.awaitGateOpenLatch = new ResettableCountDownLatch(1);

            this.eventLoopGroup = eventLoopGroup;

            Bootstrap bootstrap = type.bootStrap(this.eventLoopGroup);
            bootstrap.handler(new ChannelInitializer<T>() {

                @Override
                protected void initChannel(T ch) throws Exception {
                    ch.pipeline()
                      .addLast(KryoEventSerializer.getLengthDecoder())
                      .addLast(new KryoEventSerializer.KryoOutboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(allocator, null)))
                      .addLast(new KryoEventSerializer.KryoInboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(allocator, null)))
                      .addLast(new OpenCloseGateHandler())
                      .addLast(new WritableHandler());
                }
            });

            // on success:
            // the close future is registered
            // the polling thread is started
            bootstrap.connect(address).addListener(new ConnectListener());
        }

        /**
         * Writes the event to the channel.
         * <p/>
         * If the the gate the channel is connected to is not open yet, the events are buffered in a
         * transferQueue. If this intermediate transferQueue is full, this method blocks until the
         * gate is opened.
         * 
         * @param event
         */
        public void write(IOEvents.DataIOEvent event) {
            try {
                if (!gateOpen.get()) {
                    awaitGateOpenLatch.await();
                }

                this.transferQueue.put(event);
            } catch (InterruptedException e) {
                LOG.error("Write of event " + event + " was interrupted.", e);
            }
        }

        /**
         * Shut down the channel writer.
         */
        public void shutdown(boolean awaitExhaustion) {

            // force interrupt
            if (!awaitExhaustion) {
                // stops send, even if events left in the transferQueue
                shutdown = true;
            }

            // even if we shutdown gracefully, we have to send an interrupt
            // otherwise the thread would never return, if the transferQueue is already empty and
            // the poll thread is blocked in the take method.
            pollResult.cancel(true);

            try {
                // we can't use the return value of result cause we have to interrupt the thread
                // therefore we need the latch and the field
                pollFinished.await();
            } catch (InterruptedException e) {
                LOG.error("Receiving future from poll thread failed. Interrupt.", e);
            } finally {
                LOG.debug("CLOSE CHANNEL " + channel);
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
            LOG.debug("Event queue attached.");
            queueReady.countDown();
        }

        /**
         * Takes buffers from the context transferQueue and writes them to the channel. If the
         * channel is currently not writable, no calls to the channel are made.
         */
        private class Poll implements Runnable {

            private final Logger LOG = LoggerFactory.getLogger(Poll.class);


            /*
             * We use a Callable here to be able to shutdown single threads instead of all threads
             * managed by the executor.
             */
            @Override
            public void run() {
                try {
                    queueReady.await();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        LOG.info("Shutdown signal received. Queue was not attached.");
                        // set shutdown true, as the interrupt occurred while the queue was not
                        // attached yet.
                        shutdown = true;
                    }
                }

                long notWritable = 0l;
                long writes = 0l;
                long writeDuration = 0l;

                while (!shutdown) {
                    try {
                        if (channelWritable.get()) {

                            IOEvents.DataIOEvent dataIOEvent = transferQueue.take();

                            if (dataIOEvent.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                                LOG.debug("Data source exhausted. Shutting down poll thread.");
                                shutdown = true;
                            }

                            long start = System.nanoTime();
                            channel.writeAndFlush(dataIOEvent).syncUninterruptibly();
                            writeDuration += Math.abs(System.nanoTime() - start);
                            ++writes;
                        } else {
                            ++notWritable;
                            LOG.trace("Channel not writable.");
                        }
                    } catch (InterruptedException e) {
                        LOG.debug("Interrupted");
                        // interrupted during take command, 2. scenarios
                        // 1. interrupt while transferQueue was empty -> event == null,
                        // transferQueue == empty
                        // 2. interrupt before the transferQueue acquired the lock -> event == null,
                        // transferQueue == not empty

                        // either the thread is forced to shut down -> shutdown == true
                        // or gracefully, send events in transferQueue before shutdown -> shutdown
                        // == false
                    }
                }

                transferQueue.getMeasurementManager().add(new NumberMeasurement(MeasurementType.NUMBER,
                                                                                transferQueue.getName() + " -> Avg. write",
                                                                                (long) ((double) writeDuration / (double) writes)));
                transferQueue.getMeasurementManager().add(new NumberMeasurement(MeasurementType.NUMBER,
                                                                                transferQueue.getName() + " -> Not writable",
                                                                                notWritable));

                LOG.debug("Polling Thread is closing.");

                // signal shutdown method
                pollFinished.countDown();
            }
        }

        private class OpenCloseGateHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, IOEvents.DataIOEvent gateEvent) throws Exception {

                switch (gateEvent.type) {
                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN:
                        LOG.debug("RECEIVED GATE OPEN EVENT");

                        gateOpen.set(true);
                        awaitGateOpenLatch.countDown();

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);
                        // dispatch event to output gate

                        break;

                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE:
                        LOG.debug("RECEIVED GATE CLOSE EVENT");


                        awaitGateOpenLatch.reset();
                        gateOpen.set(false);

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);

                        // as the gate is closed, now events could be enqueued at this point
                        IOEvents.DataIOEvent closedGate =
                                new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK, srcID, dstID);
                        transferQueue.offer(closedGate);

                        break;

                    default:
                        LOG.error("RECEIVED UNKNOWN EVENT TYPE: " + gateEvent.type);
                        break;
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

                        channel = future.channel();
                        LOG.debug("Channel successfully connected.");

                        Poll pollThread = new Poll();
                        pollResult = pollThreadExecutor.submit(pollThread);

                        future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                                                                srcID,
                                                                                dstID));

                        // Disable the auto read functionality of the channel. You must not forget
                        // to enable it again!
                        future.channel().config().setAutoRead(true);

                        IOEvents.DataIOEvent event = new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP, srcID, dstID);
                        event.setPayload(ChannelWriter.this);
                        event.setChannel(future.channel());

                        dispatcher.dispatchEvent(event);
                    } else if (future.cause() != null) {
                        LOG.error("Connection attempt failed: ", future.cause());
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
                channelWritable.set(ctx.channel().isWritable());
                ctx.fireChannelActive();
            }

            @Override
            public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
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
            super();
        }

        public LocalConnection(int bufferSize) {
            super(bufferSize);
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(LocalChannel.class)
            // the mark the outbound bufferQueue has to reach in order
            // to change the writable state of a channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order
              // to change the writable state of a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }

    public static class NetworkConnection extends AbstractConnection<SocketChannel> {

        public NetworkConnection() {
            super();
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
              // -> bufferQueue size, as we always have only 1 channel per socket in the examples
              // case
              .option(ChannelOption.SO_SNDBUF, bufferSize)
              // the mark the outbound bufferQueue has to reach in order to change the writable
              // state of
              // a channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order to change the writable
              // state of
              // a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }
}
