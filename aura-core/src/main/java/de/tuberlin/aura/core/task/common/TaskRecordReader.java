package de.tuberlin.aura.core.task.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;
import de.tuberlin.aura.core.memory.MemoryView;

/**
 * Created by akunft on 14.05.14.
 */
public class TaskRecordReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final Kryo kryo;

    private final Input input;

    private byte[] buffer;

    private int memoryBaseOffset;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskRecordReader(final int bufferSize) {

        this.bufferSize = bufferSize;

        this.kryo = new Kryo();

        this.input = new FastInput();
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
        if (memView == null)
            throw new IllegalArgumentException("memView == null");

        buffer = memView.memory;

        memoryBaseOffset = memView.baseOffset;

        // TODO: do we need to flush before we select a new buffer?

        input.setBuffer(buffer);

        input.setPosition(memoryBaseOffset);
    }

    /**
     * 
     * @param recordType
     * @return
     */
    public <T> T readRecord(final Class<T> recordType) {
        // sanity check.
        if (recordType == null)
            throw new IllegalArgumentException("recordType == null");
        if (buffer == null)
            throw new IllegalStateException("buffer == null");

        return kryo.readObject(input, recordType);
    }
}
