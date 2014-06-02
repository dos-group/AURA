package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.ResettableCountDownLatch;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class DataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    private final IEventDispatcher dispatcher;

    public DataWriter(IEventDispatcher dispatcher) {

        this.dispatcher = dispatcher;
    }

    public <T extends Channel> ChannelWriter<T> bind(final UUID srcTaskID,
                                                     final UUID dstTaskID,
                                                     final OutgoingConnectionType<T> connectionType,
                                                     final SocketAddress address,
                                                     final EventLoopGroup eventLoopGroup) {

        return new ChannelWriter<>(srcTaskID, dstTaskID, connectionType, address, eventLoopGroup);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class ChannelWriter<T extends Channel> {

        // connection

        private final UUID srcID;

        private final UUID dstID;

        private Channel channel;

        private final CountDownLatch exhaustedAcknowledged = new CountDownLatch(1);

        private final CountDownLatch waitForQueueBind = new CountDownLatch(1);

        private BufferQueue<IOEvents.DataIOEvent> outboundQueue;

        // gate semantics

        private final ResettableCountDownLatch awaitGateOpenLatch;

        private AtomicBoolean gateOpen = new AtomicBoolean(false);

        final int maxConnectionRetries = 5;

        final AtomicInteger connnectionRetries = new AtomicInteger();

        public ChannelWriter(final UUID srcTaskID,
                             final UUID dstTaskID,
                             final OutgoingConnectionType<T> connectionType,
                             final SocketAddress address,
                             final EventLoopGroup eventLoopGroup) {

            this.srcID = srcTaskID;
            this.dstID = dstTaskID;
            this.awaitGateOpenLatch = new ResettableCountDownLatch(1);

            Bootstrap bootstrap = connectionType.bootStrap(eventLoopGroup);
            bootstrap.handler(connectionType.getPipeline(this));

            // bootstrap.connect(address).addListener(new ConnectListener(bootstrap, address));
            ChannelFuture future = bootstrap.connect(address);
            try {
                boolean await = future.await(30, TimeUnit.SECONDS);
                if (await) {
                    channel = future.channel();
                    LOG.debug("Channel successfully connected.");

                    future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcID, dstID));

                    // Dispatch OUTPUT_CHANNEL_CONNECTED event.
                    final IOEvents.DataIOEvent connected =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcID, dstID);
                    connected.setPayload(ChannelWriter.this);
                    connected.setChannel(channel);
                    dispatcher.dispatchEvent(connected);
                } else {
                    LOG.error("could not establish connection");
                }
            } catch (InterruptedException e) {
                LOG.error("connect was interrupted");
            }
        }

        /**
         * Writes the event to the channel.
         * <p/>
         * If the the gate the channel is connected to is not open yet, the events are buffered in a
         * outboundQueue. If this intermediate outboundQueue is full, this method blocks until the
         * gate is opened.
         * 
         * @param event
         */
        public void write(IOEvents.DataIOEvent event) {
            try {
                if (!gateOpen.get()) {
                    awaitGateOpenLatch.await();
                }

                this.outboundQueue.offer(event);
            } catch (InterruptedException e) {
                LOG.error("Write of event " + event + " was interrupted.", e);
            }
        }

        /**
         * Disconnects and closes the channel.
         * 
         * If `awaitExhaustion` is set, this method blocks until the acknowledge for the exhausted
         * event is received. If not, the channel is shut down immediately, even if there are still
         * events in the attached queue.
         * 
         * @param awaitExhaustion true, if the method should block until the exhausted event is
         *        received.
         */
        public void shutdown(boolean awaitExhaustion) {

            try {
                // wait for the receivers acknowledge before shutdown
                if (awaitExhaustion) {
                    while (!exhaustedAcknowledged.await(1, TimeUnit.MINUTES)) {
                        LOG.error("latch reached timelimit " + outboundQueue.size() + " " + channel + "(" + channel.getClass() + ")");
                        // channel.pipeline().fireChannelWritabilityChanged();
                        IOEvents.DataIOEvent event = outboundQueue.poll();
                        if (event != null) {
                            channel.writeAndFlush(event);
                        }
                    }
                }
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

        public void setOutboundQueue(BufferQueue<IOEvents.DataIOEvent> queue) {
            this.outboundQueue = queue;
            LOG.debug("Event queue attached.");
            waitForQueueBind.countDown();
        }

        /**
         * Handles all incoming events (currently gate open, gate close, exhausted acknowledge).
         */
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
                        outboundQueue.offer(closedGate);

                        break;

                    case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED_ACK:
                        LOG.debug("RECEIVED EXHAUSTED ACK EVENT");
                        exhaustedAcknowledged.countDown();
                        break;
                    default:
                        LOG.error("RECEIVED UNKNOWN EVENT TYPE: " + gateEvent.type);
                        break;
                }
            }
        }

        /**
         * If the connection was successfully established, a
         * {@link de.tuberlin.aura.core.iosystem.IOEvents.DataEventType#DATA_EVENT_INPUT_CHANNEL_CONNECTED}
         * is send to the peer and a
         * {@link de.tuberlin.aura.core.iosystem.IOEvents.DataEventType#DATA_EVENT_OUTPUT_CHANNEL_CONNECTED}
         * is dispatched.
         */
        public final class ConnectListener implements ChannelFutureListener {

            private final Bootstrap bootstrap;

            private final SocketAddress address;

            public ConnectListener(Bootstrap bootstrap, SocketAddress address) {
                this.bootstrap = bootstrap;
                this.address = address;
            }

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {

                    channel = future.channel();
                    LOG.debug("Channel successfully connected.");

                    future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcID, dstID));

                    LOG.error("connected");

                    // Dispatch OUTPUT_CHANNEL_CONNECTED event.
                    final IOEvents.DataIOEvent connected =
                            new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcID, dstID);
                    connected.setPayload(ChannelWriter.this);
                    connected.setChannel(channel);
                    dispatcher.dispatchEvent(connected);

                } else {
                    if (connnectionRetries.incrementAndGet() > maxConnectionRetries) {
                        LOG.error("Connection attempt failed: ", future.cause());
                        throw new IllegalStateException("connection attempt failed.", future.cause());
                    } else {
                        LOG.info("Connection retry (" + connnectionRetries.get() + ") ...");
                        bootstrap.connect(address).addListener(this);
                    }
                }
            }
        }

        /**
         * Binds the write observer to the outbound queue and triggers the initial write to the
         * channel.
         */
        private class ChannelActiveHandler extends ChannelInboundHandlerAdapter {

            @Override
            public void channelActive(final ChannelHandlerContext ctx) throws Exception {
                ctx.channel().eventLoop().submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        waitForQueueBind.await();
                        // bind observer
                        outboundQueue.registerObserver(new WriteableObserver(ctx));
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

            @Override
            public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {

                if (ctx.channel().isWritable()) {
                    final IOEvents.DataIOEvent event = outboundQueue.poll();
                    if (event != null) {
                        ctx.channel().writeAndFlush(event).addListener(new ChannelFutureListener() {

                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                ctx.pipeline().fireChannelWritabilityChanged();
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
                      .addLast(KryoEventSerializer.KRYO_OUTBOUND_HANDLER())
                      .addLast(KryoEventSerializer.KRYO_INBOUND_HANDLER(null))
                      .addLast(channelWriter.new OpenCloseGateHandler())
                      .addLast(channelWriter.new ChannelActiveHandler())
                      .addLast(channelWriter.new WriteHandler());
                }
            };
        }
    }
}
