package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.memory.spi.IAllocator;


public interface ITaskExecutionUnit {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract int getExecutionUnitID();

    public abstract void start();

    public abstract void stop();

    public abstract void enqueueTask(final ITaskRuntime context);

    public abstract int getNumberOfEnqueuedTasks();

    public abstract ITaskRuntime getRuntime();

    public abstract IAllocator getInputAllocator();

    public abstract IAllocator getOutputAllocator();

    public abstract Thread getExecutorThread();

    public abstract void eraseDataset();
}
