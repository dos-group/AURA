package de.tuberlin.aura.taskmanager;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.record.RowRecordModel;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

import java.security.KeyException;

/**
 *
 */
public class TaskRecordReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final Kryo kryo;

    private final Input input;

    private final BufferStream.ContinuousByteInputStream inputStream;

    private boolean isFinished = false;

    private final Class<?> recordType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskRecordReader(final ITaskDriver driver, final int gateIndex, final Class<?> recordType) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");
        if (recordType == null)
            throw new IllegalArgumentException("recordType == null");

        this.recordType = recordType;

        this.bufferSize = driver.getDataProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo();

        this.inputStream = new BufferStream.ContinuousByteInputStream();

        this.inputStream.setBufferInput(new BufferStream.BufferInput() {

            @Override
            public MemoryView get() {
                IOEvents.TransferBufferEvent inputEvent = null;
                while (inputEvent == null || !(inputEvent instanceof IOEvents.TransferBufferEvent)) {

                    try {
                        inputEvent = driver.getDataConsumer().absorb(gateIndex);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return inputEvent.buffer;
            }
        });

        this.inputStream.setBufferOutput(new BufferStream.BufferOutput() {

            @Override
            public void put(MemoryView buffer) {

                if (buffer != null) {
                    buffer.free();
                }
            }
        });

        this.input = new Input(inputStream, bufferSize);

        /*driver.getTaskManager().getIOManager().addEventListener(IOEvents.DataEventType.DATA_EVENT_RECORD_TYPE, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {

                //if (event instanceof IOEvents.RecordTypeEvent) {

                    //final IOEvents.RecordTypeEvent rte = (IOEvents.RecordTypeEvent)event;

                    //System.out.println("============> " + rte.recordType.getSimpleName());
                //}
            }
        });*/
    }

    /**
     *
     * @return
     */
    public RowRecordModel.Record readRecord() {

        RowRecordModel.Record record = null;

        try {
            record = new RowRecordModel.Record(kryo.readObject(input, recordType));
        } catch (Exception e) {
            isFinished = true;
        }

        return record;
    }

    /**
     *
     */
    public void close() {
        try {
            input.close();
            inputStream.flush();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     *
     * @return
     */
    public boolean isReaderFinished() {
        return isFinished;
    }
}
