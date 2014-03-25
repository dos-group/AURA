package de.tuberlin.aura.core.iosystem.netty;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import org.apache.log4j.Logger;

import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionUnitNioEventLoopGroup extends NioEventLoopGroup {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(ExecutionUnitNioEventLoopGroup.class);

    private final AtomicInteger childIndex = new AtomicInteger();

    private final EventExecutor[] children;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ExecutionUnitNioEventLoopGroup() {
        this(0);
    }

    public ExecutionUnitNioEventLoopGroup(int nThreads) {
        this(nThreads, null);
    }

    public ExecutionUnitNioEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, SelectorProvider.provider());
    }

    public ExecutionUnitNioEventLoopGroup(
            int nThreads, ThreadFactory threadFactory, final SelectorProvider selectorProvider) {
        super(nThreads, threadFactory, selectorProvider);

        final EventExecutor[] children = new EventExecutor[executorCount()];
        this.children = children().toArray(children);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public EventLoop next(final Channel channel) {
        LOG.info("ExecutionUnitNioEventLoopGroup.next -- " + channel.toString());
        return (EventLoop) children[Math.abs(childIndex.getAndIncrement() % children.length)];
    }

    @Override
    public ChannelFuture register(Channel channel) {
        return next(channel).register(channel);
    }

    @Override
    public ChannelFuture register(Channel channel, ChannelPromise promise) {
        return next(channel).register(channel, promise);
    }
}
