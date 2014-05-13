package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitLocalInputEventLoopGroup;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitNetworkInputEventLoopGroup;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 *
 */
public interface ITaskExecutionUnit {

    public abstract int getExecutionUnitID();

    public abstract void start();

    public abstract void stop();

    public abstract void enqueueTask(final ITaskDriver context);

    public abstract int getNumberOfEnqueuedTasks();

    public abstract ITaskDriver getCurrentTaskDriver();

    public abstract BufferAllocatorGroup getInputAllocator();

    public abstract BufferAllocatorGroup getOutputAllocator();


    public abstract ExecutionUnitNetworkInputEventLoopGroup getNetworkInputELG();

    public abstract ExecutionUnitLocalInputEventLoopGroup getLocalInputELG();

    public abstract NioEventLoopGroup getNetworkOutputELG();

    public abstract LocalEventLoopGroup getLocalOutputELG();
}
