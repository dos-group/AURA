package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.memory.spi.IAllocator;

/**
 *
 */
public interface ITaskExecutionUnit {

    public abstract int getExecutionUnitID();

    public abstract void start();

    public abstract void stop();

    public abstract void enqueueTask(final ITaskDriver context);

    public abstract int getNumberOfEnqueuedTasks();

    public abstract ITaskDriver getTaskDriver();

    public abstract IAllocator getInputAllocator();

    public abstract IAllocator getOutputAllocator();

}
