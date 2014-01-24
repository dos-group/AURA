package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

public class LocalChannelReader extends AbstractReader {
    public LocalChannelReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, LocalAddress socketAddress) {
        this(dispatcher, eventLoopGroup, socketAddress, DEFAULT_BUFFER_SIZE);
    }

    public LocalChannelReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, LocalAddress socketAddress, int bufferSize) {
        super(dispatcher, eventLoopGroup, socketAddress, bufferSize);
    }

    @Override
    public void bind() {

        try {
            ServerBootstrap b = new ServerBootstrap();
            // TODO: check if its better to use a dedicated boss and worker group instead of one for both
            b.group(eventLoopGroup)
                    .channel(LocalServerChannel.class)
                            // sets the per socket buffer size in which incoming msg are buffered until they are read
                            // TODO: get a good value here, as the size should depend on the amount of bufferQueues open
                            // TODO: so the best case is #bufferQueues * bufferSize???
                            //.childOption(ChannelOption.SO_RCVBUF, bufferSize)
                            // set a PER CHANNEL buffer in which the data is read
//                    .handler(new ChannelInboundHandlerAdapter() {
//                        @Override
//                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//                            Channel child = (Channel) msg;
//
//                            try {
//                                if (!child.config().setOption(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))) {
//                                    LOG.error("Unknown channel option: " + ChannelOption.ALLOCATOR);
//                                    throw new IllegalStateException("Unable to set allocator for channel.");
//                                }
//
//                                ctx.fireChannelRead(msg);
//
//                            } catch (Throwable t) {
//                                LOG.error("Failed to set a channel option: " + child, t);
//                                throw t;
//                            }
//                        }
//                    })
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
                        throw new IllegalStateException("could not start netty network server", future.cause());
                    }
                }
            }).sync();

            //f.channel().closeFuture().//
            // TODO: add logic for closing

        } catch (InterruptedException e) {
            LOG.error("failed to initialize network server", e);
            throw new IllegalStateException("failed to initialize network server", e);
        } finally {
            // TODO: shutdown event loop group eventLoopGroup.shutdownGracefully();
        }
    }
}
