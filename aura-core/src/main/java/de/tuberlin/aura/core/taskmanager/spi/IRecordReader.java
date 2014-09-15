package de.tuberlin.aura.core.taskmanager.spi;

import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.RowRecordModel;

/**
 *
 */
public interface IRecordReader {

    public abstract void begin();

    public abstract RowRecordModel.Record readRecord();

    public abstract Object readObject();

    public abstract void end();

    public abstract boolean finished();

    public abstract void addInputBufferAccessor(final InputBufferAccessor bufferAccessor);

    // ---------------------------------------------------
    // Inner classes.
    // ---------------------------------------------------

    public interface InputBufferAccessor {

        public abstract void accessInputBuffer(final MemoryView buffer);
    }
}
