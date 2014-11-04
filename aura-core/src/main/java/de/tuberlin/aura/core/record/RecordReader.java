package de.tuberlin.aura.core.record;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

import java.io.IOException;


public class RecordReader implements IRecordReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean isFinished = false;

    private Kryo kryo;

    private Input kryoInput;

    private BufferStream.ContinuousByteInputStream inputStream;

    private final ITaskRuntime runtime;

    private final int gateIndex;

    private int channelCount;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RecordReader(final ITaskRuntime runtime, final int gateIndex) {
        // sanity check.
        if (runtime == null)
            throw new IllegalArgumentException("runtime == null");

        this.runtime = runtime;

        this.gateIndex = gateIndex;

        final int bufferSize = runtime.getProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo(null);

        final BufferStream.ContinuousByteInputStream inputStream = new BufferStream.ContinuousByteInputStream();

        inputStream.setBufferInputHandler(new BufferStream.IBufferInputHandler() {

            @Override
            public MemoryView get() {
                try {
                    final IOEvents.TransferBufferEvent event = runtime.getConsumer().absorb(gateIndex);
                    if (event == null) {
                        isFinished = true;
                        return null;
                    }
                    return event.buffer;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        });

        inputStream.setBufferOutputHandler(new BufferStream.IBufferOutputHandler() {

            @Override
            public void put(final MemoryView buffer) {
                if (buffer != null) {
                    buffer.free();
                }
            }
        });

        this.inputStream = inputStream;

        this.kryoInput = new UnsafeInput(inputStream, bufferSize);

        ((UnsafeInput)kryoInput).setVarIntsEnabled(false);
    }

    public void begin() {

        if (runtime.getBindingDescriptor().inputGateBindings.size() >= gateIndex + 1)
            channelCount = runtime.getBindingDescriptor().inputGateBindings.get(gateIndex).size();
        else
            channelCount = 1;
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

        if (object != null && object.getClass() == RowRecordModel.RECORD_CLASS_ITERATION_END.class) {
            --channelCount;

            if (channelCount == 0) {
                kryoInput.setLimit(0);
                kryoInput.rewind();
                return null;
            }

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

    public void end() {
        try {
            inputStream.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        kryoInput.setLimit(0);
        kryoInput.rewind();
    }

    public boolean finished() {
        return isFinished;
    }

    // ---------------------------------------------------

    public void setBufferInputHandler(final BufferStream.IBufferInputHandler inputHandler) {
        inputStream.setBufferInputHandler(inputHandler);
    }

    public void setBufferOutputHandler(final BufferStream.IBufferOutputHandler outputHandler) {
        inputStream.setBufferOutputHandler(outputHandler);
    }

    public int getGateIndex() {
        return gateIndex;
    }

    public int getChannelCount() {
        return channelCount;
    }
}
