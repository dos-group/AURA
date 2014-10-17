package de.tuberlin.aura.core.taskmanager.spi;

import de.tuberlin.aura.core.memory.spi.IAllocator;

public interface ITaskRuntimeLifecycle {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void initialize(final IAllocator inputAllocator, final IAllocator outputAllocator);

    public abstract boolean execute();

    public abstract void release(boolean awaitExhaustion);
}
