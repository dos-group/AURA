package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.memory.MemoryView;

/**
 *
 */
public interface IRecordReader {

    public abstract void selectBuffer(final MemoryView memView);

    public abstract <T> T readRecord(final Class<T> recordType);
}
