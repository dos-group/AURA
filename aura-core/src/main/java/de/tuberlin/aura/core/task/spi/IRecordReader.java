package de.tuberlin.aura.core.task.spi;

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
}
