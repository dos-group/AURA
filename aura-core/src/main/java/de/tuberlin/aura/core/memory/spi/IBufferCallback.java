package de.tuberlin.aura.core.memory.spi;

import de.tuberlin.aura.core.memory.MemoryView;

/**
 *
 */
public interface IBufferCallback {

    public abstract void bufferReader(final MemoryView buffer);

}
