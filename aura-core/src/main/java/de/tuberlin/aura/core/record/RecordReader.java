package de.tuberlin.aura.core.record;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

import java.util.ArrayList;
import java.util.List;


public class RecordReader implements IRecordReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean isFinished = false;

    private final Kryo kryo;

    private final Input kryoInput;

    private final BufferStream.ContinuousByteInputStream inputStream;

    private List<InputBufferAccessor> inputBufferAccessors;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RecordReader(final ITaskRuntime driver, final int gateIndex) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("runtime == null");

        final int bufferSize = driver.getProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo(null);

        this.inputBufferAccessors = new ArrayList<>();

        final BufferStream.ContinuousByteInputStream inputStream = new BufferStream.ContinuousByteInputStream();

        inputStream.setBufferInput(new BufferStream.IBufferInput() {

            @Override
            public MemoryView get() {
                try {
                    final IOEvents.TransferBufferEvent event =  driver.getConsumer().absorb(gateIndex);
                    if (event == null) {
                        isFinished = true;
                        return null;
                    }
                    final MemoryView buffer = event.buffer;
                    for (final InputBufferAccessor bufferAccessor : inputBufferAccessors) {
                        if (buffer != null) {
                            bufferAccessor.accessInputBuffer(buffer);
                        }
                    }
                    return buffer;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        });

        inputStream.setBufferOutput(new BufferStream.IBufferOutput() {

            @Override
            public void put(final MemoryView buffer) {
                if (buffer != null) {
                    buffer.free();
                }
            }
        });

        this.inputStream = inputStream;

        this.kryoInput = new UnsafeInput(inputStream, bufferSize);
    }

    public void begin() {
    }

    public Object readObject() {
        Object object = kryo.readClassAndObject(kryoInput);
        if (object != null && object.getClass() == RowRecordModel.RECORD_CLASS_BLOCK_END.class) {
            inputStream.nextBuf();
            // stream is exhausted
            if (isFinished) {
                return null;
            }

            kryoInput.setLimit(0);
            kryoInput.rewind();
            return readObject();
        }

        return object;
    }

    public void end() {}

    public boolean finished() {
        return isFinished;
    }

    @Override
    public void addInputBufferAccessor(final InputBufferAccessor bufferAccessor) {
        // sanity check.
        if (bufferAccessor == null)
            throw new IllegalArgumentException("bufferAccessor == null");

        inputBufferAccessors.add(bufferAccessor);
    }
}
