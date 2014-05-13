package de.tuberlin.aura.core.memory.test;

/**
 *
 */
public interface IBufferMemoryManager {

    public long getUsedMemory();

    public long getFreeMemory();

    public long getTotalMemory();

    public BufferAllocatorGroup getBufferAllocatorGroup();
}
