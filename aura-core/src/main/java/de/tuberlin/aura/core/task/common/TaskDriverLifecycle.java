package de.tuberlin.aura.core.task.common;

import de.tuberlin.aura.core.memory.IAllocator;

public interface TaskDriverLifecycle {

    public abstract void startupDriver(final IAllocator inputAllocator, final IAllocator outputAllocator);

    public abstract void executeDriver();

    public abstract void teardownDriver(boolean awaitExhaustion);
}
