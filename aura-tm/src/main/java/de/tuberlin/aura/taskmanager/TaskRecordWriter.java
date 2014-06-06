package de.tuberlin.aura.taskmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.RowRecordModel;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

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
                    return driver.getDataProducer().alloc();
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

            //driver.getDataProducer().emit(0, i, new IOEvents.RecordTypeEvent(srcTaskID, dstTaskID));
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
     */
    public void end() {
        try {
            for (int i = 0; i < outputBinding.size(); ++i) {
                //kryo.writeObject(kryoOutputs.get(i), RowRecordModel.RECORD_STREAM_END.instance());
                kryoOutputs.get(i).close();
                outputStreams.get(i).close();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
