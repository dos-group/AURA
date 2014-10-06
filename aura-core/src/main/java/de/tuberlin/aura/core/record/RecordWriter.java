package de.tuberlin.aura.core.record;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordWriter implements IRecordWriter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Partitioner.IPartitioner partitioner;

    private final Kryo kryo;

    private final List<Output> kryoOutputs;

    private final List<Descriptors.AbstractNodeDescriptor> outputBinding;

    private final TypeInformation typeInformation;

    private final int channelCount;

    private Integer groupChannelIndex;

    private Map<Integer,Boolean> channelNeedsGroupEndMarkerBeforeNextWrite;

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

        this.typeInformation = typeInformation;

        this.partitioner = partitioner;

        final int bufferSize = driver.getProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo(null);

        this.kryoOutputs = new ArrayList<>();

        this.outputBinding = driver.getBindingDescriptor().outputGateBindings.get(gateIndex); // 1

        this.groupChannelIndex = null;

        this.channelNeedsGroupEndMarkerBeforeNextWrite = new HashMap<>();

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

        // handle groups in writeRecord as well (even though partitioner.partition record is not implemented yet..)

        if (typeInformation.isGrouped()) {

            if (object == GroupEndMarker.class) {
                channelNeedsGroupEndMarkerBeforeNextWrite.put(groupChannelIndex, true);
                groupChannelIndex = null;
            } else {

                Integer channelIndex;
                if (groupChannelIndex == null) {
                    if (partitioner != null) {
                        channelIndex = partitioner.partition(object, outputBinding.size());
                    } else {
                        channelIndex = 0;
                    }
                    if (channelNeedsGroupEndMarkerBeforeNextWrite.containsKey(channelIndex)
                            && channelNeedsGroupEndMarkerBeforeNextWrite.get(channelIndex)) {

                        kryo.writeClassAndObject(kryoOutputs.get(channelIndex), GroupEndMarker.class);
                        // ensure object is written to one buffer only
                        kryoOutputs.get(channelIndex).flush();

                        channelNeedsGroupEndMarkerBeforeNextWrite.put(channelIndex, false);
                    }
                } else {
                    channelIndex = groupChannelIndex;
                }

                kryo.writeClassAndObject(kryoOutputs.get(channelIndex), object);
                groupChannelIndex = channelIndex;
                // ensure object is written to one buffer only
                kryoOutputs.get(groupChannelIndex).flush();
            }
        } else {

            Integer channelIndex;
            if (partitioner != null)
                channelIndex = partitioner.partition(object, outputBinding.size());
            else
                channelIndex = 0;

            kryo.writeClassAndObject(kryoOutputs.get(channelIndex), object);
            // ensure object is written to one buffer only
            kryoOutputs.get(channelIndex).flush();
        }
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
