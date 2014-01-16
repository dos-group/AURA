package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.iosystem.buffer.FlipBufferAllocator;
import de.tuberlin.aura.core.iosystem.buffer.Util;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class ChannelReader implements IChannelReader {

    private final static Logger LOG = LoggerFactory.getLogger(ChannelReader.class);

    private static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    private final InetSocketAddress socketAddress;

    private final int bufferSize;

    private final NioEventLoopGroup eventLoopGroup;
    private final IEventDispatcher dispatcher;

    private ByteBufAllocator contextLevelAllocator;

    /**
     * Queues the single channels write their data into. There are possibly more channels that write into a single queue.
     * Each {@see de.tuberlin.aura.core.task.gates.InputGate} as exactly one queue.
     */
    private final List<BufferQueue<IOEvents.DataIOEvent>> queues;

    /**
     * In order to write the data to the correct queue, we map
     * (channel) -> queue index (in {@see queues})
     */
    private final Map<Channel, Integer> channelToQueueIndex;

    /**
     * In order to get the queue that belongs to the {@see InputGate}, we map
     * (task id, gate index) -> queue index (in {@see queues})
     */
    private final Map<Pair<UUID, Integer>, Integer> gateToQueueIndex;

    private final Map<Pair<UUID, Integer>, Channel> connectedChannels;

    public ChannelReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, InetSocketAddress socketAddress) {
        this(dispatcher, eventLoopGroup, socketAddress, DEFAULT_BUFFER_SIZE);
    }

    public ChannelReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, InetSocketAddress socketAddress, int bufferSize) {
        this.dispatcher = dispatcher;

        this.eventLoopGroup = eventLoopGroup;
        this.socketAddress = socketAddress;

        // TODO: need for concurrent queue?! after start we just read values?
        queues = new LinkedList<>();
        channelToQueueIndex = new HashMap<>();
        gateToQueueIndex = new HashMap<>();

        connectedChannels = new HashMap<>();

        if ((bufferSize & (bufferSize - 1)) != 0) {
            LOG.warn("The given buffer size is not a power of two.");
            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }
    }

    public void run() {

        try {
            ServerBootstrap b = new ServerBootstrap();
            // TODO: check if its better to use a dedicated boss and worker group instead of one for both
            b.group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                            // sets the max. number of pending, not yet fully connected (handshake) bufferQueues
                    .option(ChannelOption.SO_BACKLOG, 128)
                            // set keep alive, so idle connections are persistent
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                            // sets the per socket buffer size in which incoming msg are buffered until they are read
                            // TODO: get a good value here, as the size should depend on the amount of bufferQueues open
                            // TODO: so the best case is #bufferQueues * bufferSize???
                            //.childOption(ChannelOption.SO_RCVBUF, bufferSize)
                            // set a PER CHANNEL buffer in which the data is read
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            Channel child = (Channel) msg;

                            try {
                                if (!child.config().setOption(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))) {
                                    LOG.error("Unknown channel option: " + ChannelOption.ALLOCATOR);
                                    throw new IllegalStateException("Unable to set allocator for channel.");
                                }

                                ctx.fireChannelRead(msg);

                            } catch (Throwable t) {
                                LOG.error("Failed to set a channel option: " + child, t);
                                throw t;
                            }
                        }
                    })
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    //.addLast(new MergeBufferInHandler())
                                    //.addLast(new FixedLengthFrameDecoder(bufferSize))
                                    //.addLast(new InHandler());
                                    .addLast(new ObjectDecoder(ClassResolvers.softCachingResolver(getClass().getClassLoader())))
                                    .addLast(new DataHandler())
                                    .addLast(new EventHandler());
                        }
                    });
            //.childOption(ChannelOption.AUTO_READ, false);
            // would give every newly created channel the same allocator
            //.childOption(ChannelOption.ALLOCATOR, GlobalBufferManager.INSTANCE.getPooledAllocator());

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(socketAddress).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception {
                    if (future.isSuccess()) {
                        LOG.info("network server bound to address " + socketAddress);
                    } else {
                        LOG.error("bound attempt failed.", future.cause());
                        throw new IllegalStateException("could not start netty network server");
                    }
                }
            }).sync();

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            // TODO: shutdown eventloop group eventLoopGroup.shutdownGracefully();
        }
    }

    @Override
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex) {

        return queues.get(gateToQueueIndex.get(new Pair<UUID, Integer>(taskID, gateIndex)));
    }

    @Override
    public void setInputQueue(final UUID srcTaskID, final Channel channel, final int gateIndex, final BufferQueue<IOEvents.DataIOEvent> queue) {

        this.queues.add(queue);
        final int queueIndex = queues.size() - 1;
        channelToQueueIndex.put(channel, queueIndex);
        Pair<UUID, Integer> index = new Pair<UUID, Integer>(srcTaskID, gateIndex);
        gateToQueueIndex.put(index, queueIndex);
//        if (!connectedChannels.containsKey(index)) {
//
//        }
//        connectedChannels.get(index).add();

    }

    @Override
    public void write(final UUID taskID, final int gateIndex, final IOEvents.DataIOEvent event) {

    }

    private final class DataHandler extends SimpleChannelInboundHandler<IOEvents.DataBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataBufferEvent event) {
            //event.setChannel(ctx.channel());
            queues.get(channelToQueueIndex.get(ctx.channel())).offer(event);
        }
    }

    private final class EventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataIOEvent event)
                throws Exception {

            if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)) {
                IOEvents.GenericIOEvent<IChannelReader> connected =
                        new IOEvents.GenericIOEvent<IChannelReader>(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, ChannelReader.this, event.srcTaskID, event.dstTaskID);
                connected.setChannel(ctx.channel());
                dispatcher.dispatchEvent(connected);
            } else {
                event.setChannel(ctx.channel());
                dispatcher.dispatchEvent(event);
            }
        }
    }


    // TODO: Make use of flip buffer
    // TODO see FixedLengthFrameDecoder for more error resistant solution
    // TODO use the context lvl buffer provider here instead of i/o lvl
    private class MergeBufferInHandler extends ChannelInboundHandlerAdapter {
        private ByteBuf buf;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            buf = contextLevelAllocator.buffer(bufferSize, bufferSize);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            buf.release();
            buf = null;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg;
            // do not overflow the destination buffer
            buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));

            while (buf.readableBytes() >= bufferSize) {
                ctx.fireChannelRead(buf);
                buf.clear();

                // add rest of buffer to dest
                // sadly this is not true // guaranteed to not overflow, if we set the rcv_buffer size to buffer size
                //assert m.writableBytes() <= bufferSize;
                buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));
            }
            m.release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // TODO: proper exception handling
            cause.printStackTrace();
            ctx.close();
        }
    }
}
