package de.tuberlin.aura.core.record;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class RecordWriter implements IRecordWriter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Partitioner.IPartitioner partitioner;

    private final Kryo kryo;

    private final List<Output> kryoOutputs;

    private final List<Descriptors.AbstractNodeDescriptor> outputBinding;

    private final int channelCount;

    // block end marker
    public static byte[] BLOCK_END;

    static {
        Kryo tmpKryo = new Kryo(null);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
        Output output = new UnsafeOutput(baos);
        tmpKryo.writeClassAndObject(output, new RowRecordModel.RECORD_CLASS_BLOCK_END());
        output.flush();
        BLOCK_END = baos.toByteArray();
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RecordWriter(final ITaskRuntime driver, final TypeInformation typeInformation, final int gateIndex, final Partitioner.IPartitioner partitioner) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("runtime == null");
        if (typeInformation == null)
            throw new IllegalArgumentException("typeInformation == null");

        this.partitioner = partitioner;

        final int bufferSize = driver.getProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo(null);

        this.kryoOutputs = new ArrayList<>();

        this.outputBinding = driver.getBindingDescriptor().outputGateBindings.get(gateIndex); // 1

        this.channelCount = (partitioner != null) ? outputBinding.size() : 1;

        for (int i = 0; i < channelCount; ++i) {
            final int index = i;
            final BufferStream.ContinuousByteOutputStream os = new BufferStream.ContinuousByteOutputStream();
            final Output kryoOutput = new UnsafeOutput(os, bufferSize);
            kryoOutputs.add(kryoOutput);

            os.setBufferInput(new BufferStream.IBufferInput() {

                @Override
                public MemoryView get() {
                    try {
                        return driver.getProducer().getAllocator().allocBlocking();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });

            os.setBufferOutput(new BufferStream.IBufferOutput() {

                @Override
                public void put(MemoryView buffer) {
                    if (partitioner != null) {
                        driver.getProducer().emit(gateIndex, index, buffer);
                    } else {
                        driver.getProducer().broadcast(gateIndex, buffer);
                    }
                }
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void begin() {
    }

    public void writeObject(final Object object) {
        // sanity check.
        if (object == null)
            throw new IllegalArgumentException("object == null");

        if (object instanceof RowRecordModel.RECORD_CLASS_GROUP_END) {

            // group markers are currently not transferred across node boundaries. instead groups are expected to be
            // reduced to (partial) aggregates within tasks. reasoning: the group markers are not handled correctly by
            // the (round-robin) absorber when multiple channels have to be absorbed (e.g. after shuffling or when
            // connected tasks have different DOPs) or when regrouping is necessary after shuffling. at the same time,
            // when nothing is shuffled but tasks with same DOP are connected point-to-point, we don't see any benefit
            // in just doing the grouping in a different task.

            throw new IllegalStateException("Groups have to be folded within tasks");
        }

        Integer channelIndex = (partitioner != null) ? partitioner.partition(object, outputBinding.size()) : 0;

        kryo.writeClassAndObject(kryoOutputs.get(channelIndex), object);
        // flush Kryo's internal buffer to the actual channel buffer to ensure object is written to one buffer only
        kryoOutputs.get(channelIndex).flush();
    }

    public void end() {
        try {
            for (int i = 0; i < channelCount; ++i)
                kryoOutputs.get(i).close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void setPartitioner(final Partitioner.IPartitioner partitioner) {
        // sanity check.
        if (partitioner == null)
            throw new IllegalArgumentException("partitioner == null");

        this.partitioner = partitioner;
    }
}
