package de.tuberlin.aura.core.memory.spi;

import de.tuberlin.aura.core.memory.MemoryView;

/**
 *
 */
public interface IAllocator {

    public abstract MemoryView alloc();

    public abstract void free(final MemoryView memory);

    public abstract boolean hasFree();

    public abstract int getBufferSize();

    public abstract boolean isNotUsed();
}
