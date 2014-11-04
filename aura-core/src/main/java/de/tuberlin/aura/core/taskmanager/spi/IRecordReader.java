package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.memory.BufferStream;

public interface IRecordReader {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void begin();

    public abstract Object readObject();

    public abstract void end();

    public abstract boolean finished();

    // ---------------------------------------------------

    public void setBufferInputHandler(final BufferStream.IBufferInputHandler inputHandler);

    public void setBufferOutputHandler(final BufferStream.IBufferOutputHandler outputHandler);

    public int getGateIndex();

    public int getChannelCount();
}
