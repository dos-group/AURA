package de.tuberlin.aura.core.iosystem.netty;


import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import de.tuberlin.aura.core.descriptors.Descriptors;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

/**
 * TODO: Add default interface when switched to Java 8 {@see ExecutionUnitLocalInputEventLoopGroup}
 */
public class ExecutionUnitNetworkInputEventLoopGroup extends NioEventLoopGroup {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionUnitNetworkInputEventLoopGroup.class);

    private final AtomicInteger childIndex = new AtomicInteger();

    private final EventExecutor[] children;

    private final AtomicInteger counter = new AtomicInteger(0);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ExecutionUnitNetworkInputEventLoopGroup() {
        this(0);
    }

    public ExecutionUnitNetworkInputEventLoopGroup(int nThreads) {
        this(nThreads, null);
    }

    public ExecutionUnitNetworkInputEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, SelectorProvider.provider());
    }

    public ExecutionUnitNetworkInputEventLoopGroup(int nThreads, ThreadFactory threadFactory, final SelectorProvider selectorProvider) {
        super(nThreads, threadFactory, selectorProvider);

        final EventExecutor[] children = new EventExecutor[executorCount()];
        this.children = children().toArray(children);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private EventLoop next(final UUID srcTaskID, final List<List<Descriptors.AbstractNodeDescriptor>> inputGateBindings) {
        // Find the corresponding gate for the given channel
        String desc = "";
        int gateIndex = 0;
        boolean found = false;
        for (int g = 0; g < inputGateBindings.size(); ++g) {
            for (Descriptors.AbstractNodeDescriptor descriptor : inputGateBindings.get(g)) {
                if (descriptor.taskID.equals(srcTaskID)) {
                    desc = descriptor.name + "-" + descriptor.taskIndex;
                    gateIndex = g;
                    found = true;
                    break;
                }
            }

            if (found) {
                break;
            }
        }

        int possibilities = children.length / inputGateBindings.size();
        int base = gateIndex * possibilities;
        int index = base + Math.abs(childIndex.getAndIncrement() % possibilities);

        LOG.debug("Handle channel from {} (connected to gate {}) by EventLoop {}/{}", desc, gateIndex, index + 1, children.length);

        return (EventLoop) children[index];
    }

    public ChannelFuture register(Channel channel, final UUID srcTaskID, final List<List<Descriptors.AbstractNodeDescriptor>> inputGateBindings) {
        return next(srcTaskID, inputGateBindings).register(channel);
    }

    @Override
    public ChannelFuture register(Channel channel) {
        throw new NotImplementedException();
    }

    @Override
    public ChannelFuture register(Channel channel, ChannelPromise promise) {
        throw new NotImplementedException();
    }
}
