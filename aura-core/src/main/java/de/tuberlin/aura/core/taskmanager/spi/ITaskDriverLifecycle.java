package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.memory.spi.IAllocator;

public interface ITaskDriverLifecycle {

    public abstract void startupDriver(final IAllocator inputAllocator, final IAllocator outputAllocator);

    public abstract void executeDriver();

    public abstract void teardownDriver(boolean awaitExhaustion);
}
