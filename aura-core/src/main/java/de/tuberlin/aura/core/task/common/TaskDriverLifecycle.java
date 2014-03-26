package de.tuberlin.aura.core.task.common;


import de.tuberlin.aura.core.memory.MemoryManager;

public interface TaskDriverLifecycle {

    public abstract void startupDriver(final MemoryManager.Allocator inputAllocator, final MemoryManager.Allocator outputAllocator);

    public abstract void executeDriver();

    public abstract void teardownDriver(boolean awaitExhaustion);
}
