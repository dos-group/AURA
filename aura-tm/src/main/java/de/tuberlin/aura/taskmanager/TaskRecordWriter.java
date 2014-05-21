package de.tuberlin.aura.taskmanager;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.IRecordWriter;

public class TaskRecordWriter implements IRecordWriter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final Kryo kryo;

    private final Output output;

    private byte[] buffer;

    private int memoryBaseOffset;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskRecordWriter(final int bufferSize) {

        this.bufferSize = bufferSize;

        this.kryo = new Kryo();

        this.output = new FastOutput();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     *
     * @param memView
     */
    public void selectBuffer(final MemoryView memView) {
        // sanity check.
        if(memView == null)
            throw new IllegalArgumentException("memView == null");

        buffer = memView.memory;

        memoryBaseOffset = memView.baseOffset;

        // TODO: do we need to flush before we select a new buffer?

        output.setBuffer(buffer, bufferSize);

        output.setPosition(memoryBaseOffset);
    }

    /**
     *
     * @param record
     */
    public void writeRecord(final Object record) {
        // sanity check.
        if(record == null)
            throw new IllegalArgumentException("record == null");
        if(buffer == null)
            throw new IllegalStateException("buffer == null");

        kryo.writeObject(output, record);
    }
}
