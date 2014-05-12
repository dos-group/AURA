package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.ResettableCountDownLatch;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.statistic.MeasurementType;
import de.tuberlin.aura.core.statistic.NumberMeasurement;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;


// TODO: let the bufferQueue size be configurable
public class DataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    private final IEventDispatcher dispatcher;

    private ExecutorService pollThreadExecutor;

    public final AtomicLong exhaustedSent;

    public DataWriter(IEventDispatcher dispatcher) {

        this.exhaustedSent = new AtomicLong(0);

        this.dispatcher = dispatcher;

        this.pollThreadExecutor = Executors.newCachedThreadPool();
    }

    public <T extends Channel> ChannelWriter<T> bind(final UUID srcTaskID,
                                                     final UUID dstTaskID,
                                                     final OutgoingConnectionType<T> type,
                                                     final SocketAddress address,
                                                     final MemoryManager.Allocator allocator) {

        return new ChannelWriter<>(type, srcTaskID, dstTaskID, address, allocator);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class ChannelWriter<T extends Channel> {

        // connection

        private final UUID srcID;

        private final UUID dstID;

        private final MemoryManager.Allocator allocator;

        private Channel channel;

        // poll thread

        private volatile boolean shutdown = false;

        private final CountDownLatch pollFinished = new CountDownLatch(1);

        private final CountDownLatch queueReady = new CountDownLatch(1);

        private BufferQueue<IOEvents.DataIOEvent> transferQueue;

        // gate semantics

        private final ResettableCountDownLatch awaitGateOpenLatch;

        private AtomicBoolean gateOpen = new AtomicBoolean(false);

        public ChannelWriter(final OutgoingConnectionType<T> type,
                             final UUID srcID,
                             final UUID dstID,
                             final SocketAddress address,
                             final MemoryManager.Allocator allocator) {

            this.srcID = srcID;

            this.dstID = dstID;

            this.awaitGateOpenLatch = new ResettableCountDownLatch(1);

            this.allocator = allocator;

            IOEvents.SetupIOEvent<T> event =
                    new IOEvents.SetupIOEvent<>(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP, srcID, dstID, type, address, allocator);
            event.setPayload(this);

            dispatcher.dispatchEvent(event);
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

        public final class OpenCloseGateHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

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
        public final class ConnectListener implements ChannelFutureListener {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isDone()) {
                    if (future.isSuccess()) {

                        channel = future.channel();
                        LOG.debug("Channel successfully connected.");

                        future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                                                                srcID,
                                                                                dstID));

                        // Dispatch OUTPUT_CHANNEL_CONNECTED event.
                        final IOEvents.GenericIOEvent connected =
                                new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
                                                            ChannelWriter.this,
                                                            srcID,
                                                            dstID);
                        connected.setChannel(channel);
                        dispatcher.dispatchEvent(connected);

                    } else if (future.cause() != null) {
                        LOG.error("Connection attempt failed: ", future.cause());
                        throw new IllegalStateException("connection attempt failed.", future.cause());
                    }
                }
            }
        }

        private class ChannelActiveHandler extends ChannelInboundHandlerAdapter {

            @Override
            public void channelActive(final ChannelHandlerContext ctx) throws Exception {
                ctx.channel().eventLoop().submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        queueReady.await();
                        // bind observer
                        transferQueue.registerObserver(new WriteableObserver(ctx));
                        return null;
                    }
                }).addListener(new GenericFutureListener<Future<? super Void>>() {

                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if (future.isSuccess()) {
                            // goes to WriteHandler
                            ctx.fireChannelWritabilityChanged();
                        } else {
                            if (future.cause() != null)
                                throw new IllegalStateException(future.cause());
                        }
                    }
                });

                ctx.fireChannelActive();
            }
        }

        private class WriteHandler extends ChannelInboundHandlerAdapter {

            long notWritable = 0l;

            long writes = 0l;

            long writeDuration = 0l;

            @Override
            public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {

                if (ctx.channel().isWritable()) {
                    final IOEvents.DataIOEvent event = transferQueue.poll();
                    if (event != null) {

                        final long start = System.nanoTime();
                        ctx.channel().writeAndFlush(event).addListener(new ChannelFutureListener() {

                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                writeDuration += Math.abs(System.nanoTime() - start);
                                ++writes;

                                if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                                    LOG.debug("Data source exhausted. Shutting down poll thread.");

                                    transferQueue.getMeasurementManager().add(new NumberMeasurement(MeasurementType.NUMBER, transferQueue.getName()
                                            + " -> Avg. write", (long) ((double) writeDuration / (double) writes)));
                                    transferQueue.getMeasurementManager().add(new NumberMeasurement(MeasurementType.NUMBER, transferQueue.getName()
                                            + " -> Not writable", notWritable));

                                    pollFinished.countDown();
                                } else {
                                    ctx.pipeline().fireChannelWritabilityChanged();
                                }
                            }
                        });
                    }
                }
            }
        }
    }

    private static class WriteableObserver implements BufferQueue.QueueObserver {

        private final ChannelHandlerContext ctx;

        public WriteableObserver(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void signalNotFull() {
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void signalNotEmpty() {
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void signalNewElement() {
            ctx.fireChannelWritabilityChanged();
        }
    }


    public interface OutgoingConnectionType<T extends Channel> {

        Bootstrap bootStrap(final EventLoopGroup eventLoopGroup);

        ChannelInitializer<T> getPipeline(final ChannelWriter channelWriter);
    }

    public static class LocalConnection implements OutgoingConnectionType<LocalChannel> {

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            return new Bootstrap().group(eventLoopGroup).channel(LocalChannel.class)
            // the mark the outbound bufferQueue has to reach in order
            // to change the writable state of a channel true
                                  .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, IOConfig.NETTY_LOW_WATER_MARK)
                                  // the mark the outbound bufferQueue has to reach in order
                                  // to change the writable state of a channel false
                                  .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, IOConfig.TRANSFER_BUFFER_SIZE)
                                  .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        @Override
        public ChannelInitializer<LocalChannel> getPipeline(final ChannelWriter channelWriter) {
            return new ChannelInitializer<LocalChannel>() {

                @Override
                protected void initChannel(LocalChannel ch) throws Exception {
                    ch.pipeline()
                      .addLast(channelWriter.new OpenCloseGateHandler())
                      .addLast(channelWriter.new ChannelActiveHandler())
                      .addLast(channelWriter.new WriteHandler());
                }
            };
        }
    }

    public static class NetworkConnection implements OutgoingConnectionType<SocketChannel> {

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            return new Bootstrap().group(eventLoopGroup).channel(NioSocketChannel.class)
            // true, periodically heartbeats from tcp
                                  .option(ChannelOption.SO_KEEPALIVE, true)
                                  // false, means that messages get only sent if the size of the
                                  // data reached a relevant
                                  // amount.
                                  .option(ChannelOption.TCP_NODELAY, false)
                                  // size of the system lvl send bufferQueue PER SOCKET
                                  // -> bufferQueue size, as we always have only 1 channel per
                                  // socket in the client case
                                  .option(ChannelOption.SO_SNDBUF, IOConfig.TRANSFER_BUFFER_SIZE)
                                  // the mark the outbound bufferQueue has to reach in order to
                                  // change the writable
                                  // state of
                                  // a channel true
                                  .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, IOConfig.NETTY_LOW_WATER_MARK)
                                  // the mark the outbound bufferQueue has to reach in order to
                                  // change the writable
                                  // state of
                                  // a channel false
                                  .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, IOConfig.NETTY_HIGH_WATER_MARK)
                                  .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        @Override
        public ChannelInitializer<SocketChannel> getPipeline(final ChannelWriter channelWriter) {
            return new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                      .addLast(KryoEventSerializer.LENGTH_FIELD_DECODER())
                      .addLast(KryoEventSerializer.KRYO_OUTBOUND_HANDLER(channelWriter.allocator, null))
                      .addLast(KryoEventSerializer.KRYO_INBOUND_HANDLER(channelWriter.allocator, null))
                      .addLast(channelWriter.new OpenCloseGateHandler())
                      .addLast(channelWriter.new ChannelActiveHandler())
                      .addLast(channelWriter.new WriteHandler());
                }
            };
        }
    }
}
