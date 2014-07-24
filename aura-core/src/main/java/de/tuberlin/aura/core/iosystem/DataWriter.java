package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * A factory which is used to create
     * {@link de.tuberlin.aura.core.iosystem.DataWriter.ChannelWriter}.
     * 
     * @param dispatcher the dispatcher used for the events dispatched by the channel writer created
     *        by this data writer.
     */
    public DataWriter(IEventDispatcher dispatcher) {

        this.dispatcher = dispatcher;
    }

    /**
     * Creates a new {@link de.tuberlin.aura.core.iosystem.DataWriter.ChannelWriter} for the
     * specified arguments.
     * 
     * @param srcTaskID the UUID of the sending task
     * @param dstTaskID the UUID of the receiving task
     * @param connectionType the type of connection (e.g. local / tcp)
     * @param address the address of the receiving task
     * @param eventLoopGroup the event loop group used
     * @param <T> the type of channel this connection uses
     * @return a new data writer for the specified arguments
     */
    public <T extends Channel> ChannelWriter<T> bind(final UUID srcTaskID,
                                                     final UUID dstTaskID,
                                                     final IOutgoingConnectionType<T> connectionType,
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

        private final CountDownLatch waitForExhaustedAcknowledge = new CountDownLatch(1);

        private final CountDownLatch waitForQueueBind = new CountDownLatch(1);

        // connection

        final int maxConnectionRetries = 5;

        int connectionRetries = 0;

        private BufferQueue<IOEvents.DataIOEvent> outboundQueue;

        // gate semantics

        private final ResettableCountDownLatch waitForGateOpen;

        private AtomicBoolean isGateOpen = new AtomicBoolean(false);

        public ChannelWriter(final UUID srcTaskID,
                             final UUID dstTaskID,
                             final IOutgoingConnectionType<T> connectionType,
                             final SocketAddress address,
                             final EventLoopGroup eventLoopGroup) {

            this.srcID = srcTaskID;
            this.dstID = dstTaskID;
            this.waitForGateOpen = new ResettableCountDownLatch(1);

            Bootstrap bootstrap = connectionType.bootStrap(eventLoopGroup);
            bootstrap.handler(connectionType.getPipeline(this));

            // bootstrap.connect(address).addListener(new ConnectListener(bootstrap, address));
            // TODO: async. method with listener did not work sadly, investigate why
            try {
                while (connectionRetries++ < maxConnectionRetries) {

                    ChannelFuture future = bootstrap.connect(address);

                    boolean await = future.await(30, TimeUnit.SECONDS);
                    if (await) {
                        channel = future.channel();
                        LOG.debug("Channel successfully connected.");

                        future.channel().writeAndFlush(new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                                                                srcID,
                                                                                dstID));

                        // Dispatch OUTPUT_CHANNEL_CONNECTED event.
                        final IOEvents.DataIOEvent connected =
                                new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcID, dstID);
                        connected.setPayload(ChannelWriter.this);
                        connected.setChannel(channel);
                        dispatcher.dispatchEvent(connected);
                        // connection was successful
                        break;
                    } else {
                        LOG.info("Connection retry (" + connectionRetries + ") ...");
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Connection attempt was interrupted");
            }
        }

        /**
         * Writes an event to the channel.
         *
         * If the gate is not yet open, this method will block until the gate is open.
         * Only the endpoint of the channel can open gate.
         * 
         * @param event The event that is written on the channel.
         */
        public void write(IOEvents.DataIOEvent event) {
            try {
                if (!isGateOpen.get()) {
                    waitForGateOpen.await();
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
                    while (!waitForExhaustedAcknowledge.await(1, TimeUnit.MINUTES)) {
                        LOG.warn("Latch reached timelimit " + outboundQueue.size() + " " + channel + "(" + channel.getClass() + ")");
                        channel.pipeline().fireChannelWritabilityChanged();
                        // IOEvents.DataIOEvent event = outboundQueue.poll();
                        // if (event != null) {
                        // channel.writeAndFlush(event);
                        // }
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

        /**
         * Sets the outbound queue for this channel writer.
         * 
         * @param queue the queue for the events to be written by this channel writer
         */
        public void setOutboundQueue(BufferQueue<IOEvents.DataIOEvent> queue) {
            this.outboundQueue = queue;
            LOG.debug("Event queue attached.");
            waitForQueueBind.countDown();
        }

        // ---------------------------------------------------
        // NETTY CHANNEL HANDLER
        // ---------------------------------------------------

        /**
         * Handles all incoming events (currently gate open, gate close, exhausted acknowledge).
         */
        public final class OpenCloseGateHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, IOEvents.DataIOEvent gateEvent) throws Exception {

                switch (gateEvent.type) {
                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN:
                        LOG.debug("RECEIVED GATE OPEN EVENT");

                        isGateOpen.set(true);
                        waitForGateOpen.countDown();

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);
                        // dispatch event to output gate

                        break;

                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE:
                        LOG.debug("RECEIVED GATE CLOSE EVENT");

                        waitForGateOpen.reset();
                        isGateOpen.set(false);

                        gateEvent.setChannel(ctx.channel());
                        dispatcher.dispatchEvent(gateEvent);

                        // as the gate is closed, now events could be enqueued at this point
                        IOEvents.DataIOEvent closedGate =
                                new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK, srcID, dstID);
                        outboundQueue.offer(closedGate);

                        break;

                    case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED_ACK:
                        LOG.debug("RECEIVED EXHAUSTED ACK EVENT");
                        waitForExhaustedAcknowledge.countDown();
                        break;
                    default:
                        LOG.error("RECEIVED UNKNOWN EVENT TYPE: " + gateEvent.type);
                        break;
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
                            //if (future.cause() != null)
                            throw new IllegalStateException(future.cause());
                        }
                    }
                });

                ctx.fireChannelActive();
            }
        }

        /**
         * Tries to write queued events in the oubound queue to the netty channel if it is currently
         * writable.
         * 
         * When the write of an event is finished
         */
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


    public interface IOutgoingConnectionType<T extends Channel> {

        Bootstrap bootStrap(final EventLoopGroup eventLoopGroup);

        ChannelInitializer<T> getPipeline(final ChannelWriter channelWriter);
    }

    public static class LocalConnection implements IOutgoingConnectionType<LocalChannel> {

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
                      .addLast(new SerializationHandler.LocalTransferBufferCopyHandler(null))
                      .addLast(channelWriter.new OpenCloseGateHandler())
                      .addLast(channelWriter.new ChannelActiveHandler())
                      .addLast(channelWriter.new WriteHandler());
                }
            };
        }
    }

    public static class NetworkConnection implements IOutgoingConnectionType<SocketChannel> {

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
                      .addLast(SerializationHandler.LENGTH_FIELD_DECODER())
                      .addLast(SerializationHandler.KRYO_OUTBOUND_HANDLER())
                      .addLast(SerializationHandler.KRYO_INBOUND_HANDLER(null))
                      .addLast(channelWriter.new OpenCloseGateHandler())
                      .addLast(channelWriter.new ChannelActiveHandler())
                      .addLast(channelWriter.new WriteHandler());
                }
            };
        }
    }
}
