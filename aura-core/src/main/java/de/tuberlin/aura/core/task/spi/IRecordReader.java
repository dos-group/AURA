package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.iosystem.IOEvents;

/**
 *
 */
public interface IRecordReader {

    public abstract void selectBuffer(final IOEvents.TransferBufferEvent transferBuffer);

    public abstract <T> T readRecord(final Class<T> recordType);
}
