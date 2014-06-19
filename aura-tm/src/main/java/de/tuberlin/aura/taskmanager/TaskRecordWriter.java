package de.tuberlin.aura.taskmanager;

import java.io.Serializable;
import java.util.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Output;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.RowRecordModel;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import org.apache.commons.lang3.ArrayUtils;

public class TaskRecordWriter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ITaskDriver driver;

    private final int bufferSize;

    private final RowRecordModel.Partitioner partitioner;

    private final Kryo kryo;

    private final List<BufferStream.ContinuousByteOutputStream> outputStreams;

    private final List<Output> kryoOutputs;

    private final List<Descriptors.AbstractNodeDescriptor> outputBinding;

    private final Class<?> recordType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * 
     * @param driver
     * @param partitioner
     */
    public TaskRecordWriter(final ITaskDriver driver, final Class<?> recordType, final RowRecordModel.Partitioner partitioner) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");
        if (recordType == null)
            throw new IllegalArgumentException("recordType == null");
        if (partitioner == null)
            throw new IllegalArgumentException("partitioner == null");

        this.driver = driver;

        this.recordType = recordType;

        this.partitioner = partitioner;

        this.bufferSize = driver.getDataProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo();

        this.outputStreams = new ArrayList<>();

        this.kryoOutputs = new ArrayList<>();

        this.outputBinding = driver.getBindingDescriptor().outputGateBindings.get(0);

        for (int i = 0; i < outputBinding.size(); ++i) {

            final int index = i;
            
            final BufferStream.ContinuousByteOutputStream os = new BufferStream.ContinuousByteOutputStream();

            outputStreams.add(os);

            final Output kryoOutput = new Output(os, bufferSize);

            kryoOutputs.add(kryoOutput);

            os.setBufferInput(new BufferStream.BufferInput() {

                @Override
                public MemoryView get() {
                    try {
                        return driver.getDataProducer().getAllocator().allocBlocking();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });

            os.setBufferOutput(new BufferStream.BufferOutput() {

                @Override
                public void put(MemoryView buffer) {

                    final UUID srcTaskID = driver.getNodeDescriptor().taskID;

                    final UUID dstTaskID = driver.getInvokeable().getTaskID(0, index);

                    final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, buffer);

                    driver.getDataProducer().emit(0, index, outEvent);
                }
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     *
     */
    public void begin() {

        final UUID srcTaskID = driver.getNodeDescriptor().taskID;

        for (int i = 0; i < outputBinding.size(); ++i) {

            final UUID dstTaskID = driver.getInvokeable().getTaskID(0, i);

            final IOEvents.DataIOEvent event = new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_RECORD_TYPE, srcTaskID, dstTaskID);

            final byte[] tmp = RowRecordModel.RecordTypeBuilder.getRecordByteCode(recordType);

            event.setPayload(new Pair<String, Byte[]>(recordType.getName(), ArrayUtils.toObject(tmp)));

            driver.getDataProducer().emit(0, i, event);
        }
    }

    /**
     * 
     * @param record
     */
    public void writeRecord(final RowRecordModel.Record record) {
        // sanity check.
        if(record == null)
            throw new IllegalArgumentException("record == null");

        final int channelIndex = partitioner.partition(record, outputBinding.size());

        kryo.writeObject(kryoOutputs.get(channelIndex), record.instance());
    }

    /**
     *
     * @param object
     */
    public void writeObject(final Object object) {
        // sanity check.
        if(object == null)
            throw new IllegalArgumentException("object == null");

        final int channelIndex = partitioner.partition(object, outputBinding.size());

        kryo.writeClassAndObject(kryoOutputs.get(channelIndex), object);
    }

    /**
     * 
     */
    public void end() {
        try {
            for (int i = 0; i < outputBinding.size(); ++i) {
                kryo.writeClassAndObject(kryoOutputs.get(i), new RowRecordModel.RECORD_CLASS_STREAM_END());
                kryoOutputs.get(i).close();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
