package de.tuberlin.aura.core.dataflow.datasets;


import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.RecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SerializedImmutableDataset<E> extends AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private IRecordReader internalReader;

    private List<MemoryView> dataBuffers;

    private ITaskRuntime runtime;

    private int bufferIdx;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SerializedImmutableDataset(final IExecutionContext context) {
        super(context);

        this.dataBuffers = new ArrayList<>();

        this.runtime = context.getRuntime();

        this.bufferIdx = 0;

        this.internalReader = new RecordReader(runtime, 0);

        this.internalReader.setBufferInputHandler(new BufferStream.IBufferInputHandler() {

            @Override
            public MemoryView get() {
                if (bufferIdx < dataBuffers.size())
                    return dataBuffers.get(bufferIdx++);
                else
                    return null;
            }
        });

        this.internalReader.setBufferOutputHandler(new BufferStream.IBufferOutputHandler() {

            @Override
            public void put(final MemoryView buffer) {
                if (buffer == null) {
                    throw new IllegalStateException();
                }
            }
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void produceDataset(final int gateIndex) {

        try {

            IOEvents.TransferBufferEvent event = runtime.getConsumer().absorb(gateIndex);

            while (event != null) {
                dataBuffers.add(event.buffer);
                event = runtime.getConsumer().absorb(gateIndex);
            }

        } catch(InterruptedException ie) {
            throw new IllegalStateException(ie);
        }
    }

    public void consumeDataset(final IRecordWriter writer) {

        bufferIdx = 0;

        internalReader.begin();

        writer.begin();

        Object object = internalReader.readObject();

        while (object != null) {

            writer.writeObject(object);

            object = internalReader.readObject();
        }

        writer.end();

        internalReader.end();
    }

    public void releaseDataset() {

        for (final MemoryView buffer : dataBuffers) {
            buffer.free();
        }

        dataBuffers.clear();
    }

    // ---------------------------------------------------

    @Override
    public void add(E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<E> getData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        dataBuffers.clear();
    }

    @Override
    public void setData(Collection<E> data) {
        throw new UnsupportedOperationException();
    }
}
