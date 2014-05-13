package de.tuberlin.aura.core.memory;

/**
 *
 */
public interface IBufferMemoryManager {

    public long getUsedMemory();

    public long getFreeMemory();

    public long getTotalMemory();

    public BufferAllocatorGroup getBufferAllocatorGroup();
}
