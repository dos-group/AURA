package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.iosystem.buffer.FlipBufferAllocator;
import de.tuberlin.aura.core.iosystem.buffer.Util;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.BlockingQueue;

public class Server {

    private final static InternalLogger LOG = InternalLoggerFactory.getInstance(Server.class);

    private static final int DEFAULT_BUFFER_SIZE = Util.nextPowerOf2(64 << 10); //64 << 10;

    private final int port;

    private final int bufferSize;

    private BlockingQueue<ByteBuf> queue;

    private ByteBufAllocator contextLevelAllocator;

    public Server(int port) {
        this(port, DEFAULT_BUFFER_SIZE);
    }

    public Server(int port, int bufferSize) {
        this.port = port;
        this.bufferSize = bufferSize;
    }

    public void run() {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
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
                                }

                                ctx.fireChannelRead(msg);

                            } catch (Throwable t) {
                                LOG.warn("Failed to set a channel option: " + child, t);
                                throw new Exception("Unable to set allocator.");
                            }
                        }
                    })
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new MergeBufferInHandler())
                                            //.addLast(new FixedLengthFrameDecoder(bufferSize))
                                    .addLast(new InHandler());
                        }
                    });
            //.childOption(ChannelOption.AUTO_READ, false);
            // would give every newly created channel the same allocator
            //.childOption(ChannelOption.ALLOCATOR, GlobalBufferManager.INSTANCE.getPooledAllocator());

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private class InHandler extends ChannelInboundHandlerAdapter {

        boolean called = false;

        long start, end;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //System.out.println("channelRead");

            //To change body of overridden methods use File | Settings | File Templates.
            if (!called) {
                called = true;
                start = System.currentTimeMillis();
            }

            ByteBuf buffer = (ByteBuf) msg;
            long sequenceNumber = buffer.getLong(0);
            //LOG.debug("id: " + sequenceNumber);

            if (sequenceNumber % 1000 == 0) {
                called = false;
                end = System.currentTimeMillis() - start;
                LOG.error("ms: " + end);
            }

            //System.out.println(sequenceNumber);

            //super.channelRead(ctx, msg);

            //queue.offer((ByteBuf) msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.error("ex caught", cause);
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
//            buf.release();
//            buf = null;
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
