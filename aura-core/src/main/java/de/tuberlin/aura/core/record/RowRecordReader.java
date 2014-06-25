package de.tuberlin.aura.core.record;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.ArrayUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.Input;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferStream;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.IRecordReader;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

/**
 *
 */
public class RowRecordReader implements IRecordReader {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ITaskDriver driver;

    private final int bufferSize;

    private boolean isFinished = false;

    private Class<?> recordType;

    private final Lock threadLock = new ReentrantLock();

    private final Condition condition = threadLock.newCondition();

    private final int gateIndex;

    private final Kryo kryo;


    private final List<Input> kryoInputs = new ArrayList<>();

    private final List<BufferStream.ContinuousByteInputStream> inputStreams = new ArrayList<>();

    private int selectedChannel = 0;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------


    public RowRecordReader(final ITaskDriver driver, final int gateIndex) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");

        this.driver = driver;

        this.gateIndex = gateIndex;

        this.bufferSize = driver.getDataProducer().getAllocator().getBufferSize();

        this.kryo = new Kryo();


        for(final Descriptors.AbstractNodeDescriptor node : driver.getBindingDescriptor().inputGateBindings.get(gateIndex)) {

            final BufferStream.ContinuousByteInputStream inputStream = new BufferStream.ContinuousByteInputStream();

            inputStream.setBufferInput(new BufferStream.BufferInput() {

                private final int channelIndex = driver.getDataConsumer().getChannelIndexFromTaskID(node.taskID);

                @Override
                public MemoryView get() {
                    try {

                        selectedChannel = ++selectedChannel % kryoInputs.size();

                        return driver.getDataConsumer().absorb(gateIndex, channelIndex).buffer;

                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });

            inputStream.setBufferOutput(new BufferStream.BufferOutput() {

                @Override
                public void put(final MemoryView buffer) {
                    if (buffer != null) {
                        buffer.free();
                    }
                }
            });

            inputStreams.add(inputStream);

            kryoInputs.add(new FastInput(inputStream, bufferSize));
        }

        driver.getTaskManager().getIOManager().addEventListener(IOEvents.DataEventType.DATA_EVENT_RECORD_TYPE, new RecordTypeEventHandler());
    }

    /**
     *
     */
    public void begin() {
        threadLock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            threadLock.unlock();
        }
    }

    /**
     *
     * @return
     */
    public RowRecordModel.Record readRecord() {

        Object object = null;

        try {

            object = kryo.readClassAndObject(kryoInputs.get(selectedChannel));;

            if(object != null && object.getClass() == RowRecordModel.RECORD_CLASS_STREAM_END.class) {

                kryoInputs.remove(selectedChannel);

                inputStreams.remove(selectedChannel);

                if (kryoInputs.size() == 0) {
                    isFinished = true;
                    return null;
                }

                selectedChannel = ++selectedChannel % kryoInputs.size();

                return null;
            }

        } catch (Exception e) {
            isFinished = true;
        }

        return new RowRecordModel.Record(object);
    }

    /**
     *
     * @return
     */
    public Object readObject() {

        Object object = null;

        try {
            object = kryo.readClassAndObject(kryoInputs.get(selectedChannel));

            if(object != null && object.getClass() == RowRecordModel.RECORD_CLASS_STREAM_END.class) {

                kryoInputs.get(selectedChannel).close();

                kryoInputs.remove(selectedChannel);

                inputStreams.remove(selectedChannel);

                if (kryoInputs.size() == 0) {
                    isFinished = true;
                    return null;
                }

                selectedChannel = ++selectedChannel % kryoInputs.size();

                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return object;
    }

    /**
     *
     */
    public void end() {
    }

    /**
     *
     * @return
     */
    public boolean finished() {
        return isFinished;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    public class RecordTypeEventHandler implements IEventHandler {

        @Override
        public void handleEvent(Event event) {

            final IOEvents.DataIOEvent rte = (IOEvents.DataIOEvent)event;

            final Pair<String, Byte[]> recordTypeDesc = (Pair<String, Byte[]>)rte.getPayload();

            try {

                recordType = Class.forName(recordTypeDesc.getFirst());

            } catch (ClassNotFoundException e) {

                final byte[] byteCode = ArrayUtils.toPrimitive(recordTypeDesc.getSecond());

                recordType = new ClassLoader(this.getClass().getClassLoader()) {

                    public Class<?> defineClass() {
                        return defineClass(recordTypeDesc.getFirst(), byteCode, 0, byteCode.length);
                    }

                }.defineClass();

                RowRecordModel.RecordTypeBuilder.addRecordType(recordType, byteCode);
            }

            threadLock.lock();
                condition.signal();
            threadLock.unlock();
        }
    }

}
