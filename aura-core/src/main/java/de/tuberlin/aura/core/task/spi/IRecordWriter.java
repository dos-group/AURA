package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.iosystem.IOEvents;

/**
 *
 */
public interface IRecordWriter {

    public abstract void selectBuffer(final IOEvents.TransferBufferEvent transferBuffer);

    public abstract void writeRecord(final Object record);
}
