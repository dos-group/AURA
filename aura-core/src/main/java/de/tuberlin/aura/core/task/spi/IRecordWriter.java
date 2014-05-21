package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.memory.MemoryView;

/**
 *
 */
public interface IRecordWriter {

    public abstract void selectBuffer(final MemoryView memView);

    public abstract void writeRecord(final Object record);
}
