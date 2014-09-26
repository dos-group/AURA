package de.tuberlin.aura.core.record;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskDriver;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;

public class RowRecordWriter implements IRecordWriter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ITaskDriver driver;

    private Partitioner.IPartitioner partitioner;

    private final Kryo kryo;

    private final List<Output> kryoOutputs;

    private final List<Descriptors.AbstractNodeDescriptor> outputBinding;

    private final TypeInformation typeInformation;

    private final int gateIndex;

    private Integer groupChannelIndex;

    private Map<Integer,Boolean> channelNeedsGroupEndMarkerBeforeNextWrite;

    // block end marker
    public static byte[] BLOCK_END;
    static {
        Kryo k = new Kryo(null);
        ByteArrayOutputStream s = new ByteArrayOutputStream(1000);
        Output out = new FastOutput(s);
        k.writeClassAndObject(out, new RowRecordModel.RECORD_CLASS_BLOCK_END());
        out.flush();
        BLOCK_END = s.toByteArray();
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RowRecordWriter(final ITaskDriver driver, final TypeInformation typeInformation, final int gateIndex, final Partitioner.IPartitioner partitioner) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");
        if (typeInformation == null)
            throw new IllegalArgumentException("typeInformation == null");

        this.driver = driver;

        this.typeInformation = typeInformation;

        this.gateIndex = gateIndex;

        this.partitioner = partitioner;

        final int bufferSize = driver.getDataProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo(null);

        this.kryoOutputs = new ArrayList<>();

        this.outputBinding = driver.getBindingDescriptor().outputGateBindings.get(gateIndex); // 1

        this.groupChannelIndex = null;

        this.channelNeedsGroupEndMarkerBeforeNextWrite = new HashMap<>();

        final int channelCount;
        if (partitioner != null) {
            channelCount = outputBinding.size();
        } else {
            channelCount = 1;
        }

        for (int i = 0; i < channelCount; ++i) {

            final int index = i;

            final BufferStream.ContinuousByteOutputStream os = new BufferStream.ContinuousByteOutputStream();

            final Output kryoOutput = new FastOutput(os, bufferSize);

            kryoOutputs.add(kryoOutput);

            os.setBufferInput(new BufferStream.IBufferInput() {

                @Override
                public MemoryView get() {
                    try {
                        return driver.getDataProducer().getAllocator().allocBlocking();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });

            os.setBufferOutput(new BufferStream.IBufferOutput() {

                @Override
                public void put(MemoryView buffer) {

                    if (partitioner != null) {
                        driver.getDataProducer().emit(gateIndex, index, buffer);
                    } else {
                        driver.getDataProducer().broadcast(gateIndex, buffer);
                    }
                }
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void begin() {

        final UUID srcTaskID = driver.getNodeDescriptor().taskID;

        for (int i = 0; i < outputBinding.size(); ++i) {

            final UUID dstTaskID = outputBinding.get(i).taskID;

            final IOEvents.DataIOEvent event = new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_RECORD_TYPE, srcTaskID, dstTaskID);

            final byte[] tmp = RowRecordModel.RecordTypeBuilder.getRecordByteCode(typeInformation.type);

            event.setPayload(new Pair<>(typeInformation.type.getName(), ArrayUtils.toObject(tmp)));

            driver.getDataProducer().emit(gateIndex, i, event);
        }
    }

    public void writeRecord(final RowRecordModel.Record record) {
        // sanity check.
        if (record == null)
            throw new IllegalArgumentException("record == null");

        final int channelIndex;
        if (partitioner != null) {
            channelIndex = partitioner.partition(record, outputBinding.size());
        } else {
            channelIndex = 0;
        }

        kryo.writeObject(kryoOutputs.get(channelIndex), record.instance());
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

            if (partitioner != null) {
                channelIndex = partitioner.partition(object, outputBinding.size());
            } else {
                channelIndex = 0;
            }

            kryo.writeClassAndObject(kryoOutputs.get(channelIndex), object);
            // ensure object is written to one buffer only
            kryoOutputs.get(channelIndex).flush();
        }
    }

    public void end() {
        try {

            final int channelCount;
            if (partitioner != null) {
                channelCount = outputBinding.size();
            } else {
                channelCount = 1;
            }

            for (int i = 0; i < channelCount; ++i) {
                kryoOutputs.get(i).close();
            }

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
