package de.tuberlin.aura.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

/**
 *
 */
public class DataStorageDriver extends AbstractInvokeable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<MemoryView> bufferStorage;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DataStorageDriver(ITaskDriver taskDriver, IDataProducer producer, IDataConsumer consumer, Logger LOG) {
        super(taskDriver, producer, consumer, LOG);

        this.bufferStorage = new ArrayList<MemoryView>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {

    }

    @Override
    public void open() throws Throwable {
        consumer.openGate(0);
    }

    @Override
    public void run() throws Throwable {

        while (!consumer.isExhausted()) {

            final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);

            if (inEvent != null) {

                bufferStorage.add(inEvent.buffer);
            }
        }

        consumer.closeGate(0);

        driver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
    }

    @Override
    public void close() throws Throwable {

        if (hasStoredBuffers()) {

            driver.getBindingDescriptor().inputGateBindings.clear();

            driver.getBindingDescriptor().outputGateBindings.clear();

            driver.joinDispatcherThread();
        }
    }

    @Override
    public void release() throws Throwable {

    }

    /**
     *
     * @param outputBinding
     */
    public void createOutputBinding(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {
        // sanity check.
        if(outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");

        driver.getTaskStateMachine().reset();

        driver.getBindingDescriptor().addOutputGateBinding(outputBinding);

        consumer.bind(driver.getBindingDescriptor().inputGateBindings, consumer.getAllocator());

        producer.bind(driver.getBindingDescriptor().outputGateBindings, producer.getAllocator());

        driver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {

                        emitStoredBuffers(0);
                    }
                });
    }

    /**
     *
     * @param buffer
     */
    public void store(final MemoryView buffer) {
        // sanity check.
        if(buffer == null)
            throw new IllegalArgumentException("buffer == null");

        bufferStorage.add(buffer);
    }

    /**
     *
     * @return
     */
    public boolean hasStoredBuffers() {
        return bufferStorage.size() > 0;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     * @param gateIdx
     */
    private void emitStoredBuffers(final int gateIdx) {

        final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(gateIdx);
        for (int channelIdx = 0; channelIdx < outputs.size(); ++channelIdx) {
            final UUID outputTaskID = driver.getBindingDescriptor().outputGateBindings.get(gateIdx).get(channelIdx).taskID;;

            for(final MemoryView buffer : bufferStorage) {

                final IOEvents.TransferBufferEvent outputBuffer =
                        new IOEvents.TransferBufferEvent(driver.getNodeDescriptor().taskID, outputTaskID, buffer);

                producer.emit(gateIdx, channelIdx, outputBuffer);
            }
        }

        producer.done();

        driver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
    }
}
