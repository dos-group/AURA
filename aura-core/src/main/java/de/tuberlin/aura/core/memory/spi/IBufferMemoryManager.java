package de.tuberlin.aura.core.memory.spi;

import de.tuberlin.aura.core.memory.BufferAllocatorGroup;

/**
 *
 */
public interface IBufferMemoryManager {

    public long getUsedMemory();

    public long getFreeMemory();

    public long getTotalMemory();

    public BufferAllocatorGroup getBufferAllocatorGroup();
}
