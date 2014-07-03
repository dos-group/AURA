package de.tuberlin.aura.taskmanager;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.gates.OutputGate;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.storage.DataStorageDriver;

/**
 *
 */
public final class TaskDataProducer implements IDataProducer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(TaskDataProducer.class);

    private final ITaskDriver driver;

    private final List<OutputGate> outputGates;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private final IEventHandler producerEventHandler;

    private IAllocator allocator;


    //private final List<MemoryView> bufferStorage;

    private List<List<Descriptors.AbstractNodeDescriptor>> outputBinding;

    private DataStorageDriver dataStorage = null;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataProducer(final ITaskDriver driver) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");

        this.driver = driver;

        // event handling.
        this.producerEventHandler = new ProducerEventHandler();

        this.driver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, producerEventHandler);

        //this.bufferStorage = new ArrayList<MemoryView>();

        this.outputGates = new ArrayList<>();

        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     *
     * @param outputBinding
     * @param allocator
     */
    @Override
    public void bind(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding, final IAllocator allocator) {
        // sanity check.
        if (outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");
        if(allocator == null)
            throw new IllegalArgumentException("allocator == null");

        this.outputBinding = outputBinding; // TODO: copy binding?

        this.allocator = allocator;

        this.taskIDToGateIndex.clear();

        this.channelIndexToTaskID.clear();

        createOutputMappings(outputBinding);

        createOutputGates(outputBinding);

        connectOutputDataChannels(outputBinding);

        if (outputBinding.size() == 0 && dataStorage == null) {
            dataStorage = new DataStorageDriver(driver, this, driver.getDataConsumer(), driver.getLOG());
        }
    }



    /**
     * @param gateIndex
     * @param channelIndex
     * @param event
     */
    public void emit(final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");

        if (outputBinding.size() == 0)
            throw new IllegalStateException("no output binding");

        outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
    }

    /**
     * @param gateIndex
     * @param channelIndex
     * @param buffer
     */
    public void emit(final int gateIndex, final int channelIndex, final MemoryView buffer) {
        // sanity check.
        if (buffer == null)
            throw new IllegalArgumentException("buffer == null");

        if (outputBinding.size() == 0)
            throw new IllegalStateException("no output binding");

        final UUID srcTaskID = driver.getNodeDescriptor().taskID;

        final UUID dstTaskID = driver.getBindingDescriptor().outputGateBindings.get(gateIndex).get(channelIndex).taskID;

        final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, buffer);

        outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
    }

    /**
     *
     * @param gateIndex
     * @param buffer
     */
    public void broadcast(final int gateIndex, final MemoryView buffer) {
        // sanity check.
        if (buffer == null)
            throw new IllegalArgumentException("buffer == null");

        final UUID srcTaskID = driver.getNodeDescriptor().taskID;

        buffer.setRefCount(outputBinding.get(gateIndex).size());

        for (int i = 0; i < outputBinding.get(gateIndex).size(); ++i) {

            final UUID dstTaskID = driver.getBindingDescriptor().outputGateBindings.get(gateIndex).get(i).taskID;

            final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, buffer);

            outputGates.get(gateIndex).writeDataToChannel(i, event);
        }
    }

    /**
     *
     * @param buffer
     */
    @Override
    public void store(final MemoryView buffer) {
        // sanity check.
        if(buffer == null)
            throw new IllegalArgumentException("buffer == null");

        dataStorage.store(buffer);
    }

    /**
     *
     * @return
     */
    @Override
    public boolean hasStoredBuffers() {
        return dataStorage != null && dataStorage.hasStoredBuffers();
    }

    /**
     *
     * @return
     */
    @Override
    public AbstractInvokeable getStorage() {
        return dataStorage;
    }

    /**
     *
     */
    public void done() {
        final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);

        for (int index = 0; index < outputs.size(); ++index) {
            final UUID outputTaskID = driver.getBindingDescriptor().outputGateBindings.get(0).get(index).taskID;

            final IOEvents.DataIOEvent exhaustedEvent =
                    new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, driver.getNodeDescriptor().taskID, outputTaskID);

            emit(0, index, exhaustedEvent);
        }
    }

    /**
     * @param awaitExhaustion
     */
    public void shutdownProducer(boolean awaitExhaustion) {
        if (outputGates != null) {
            for (final OutputGate og : outputGates) {
                // TODO: maybe replace with event?!
                for (final DataWriter.ChannelWriter channelWriter : og.getAllChannelWriter()) {
                    channelWriter.shutdown(awaitExhaustion);
                }
            }
        }

        // taskIDToGateIndex.clear();
        // channelIndexToTaskID.clear();
        // dispatcher.removeAllEventListener(); // TODO:
    }

    /**
     * @param channelIndex
     * @return
     */
    public UUID getOutputTaskIDFromChannelIndex(int channelIndex) {
        return channelIndexToTaskID.get(channelIndex);
    }

    /**
     * Return the gate index for the corresponding task ID.
     * 
     * @param taskID The unique ID of a connected task.
     * @return The gate index or null if no suitable mapping exists.
     */
    public int getOutputGateIndexFromTaskID(final UUID taskID) {
        return taskIDToGateIndex.get(taskID);
    }

    /**
     *
     * @return
     */
    /*@Override
    public boolean hasStoredBuffers() {
        return bufferStorage.size() > 0;
    }*/


    /**
     *
     * @return
     */
    @Override
    public IAllocator getAllocator() {
        return allocator;
    }

    /**
     *
     * @param gateIdx
     */
   /* @Override
    public void emitStoredBuffers(final int gateIdx) {

        final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(gateIdx);
        for (int channelIdx = 0; channelIdx < outputs.size(); ++channelIdx) {
            final UUID outputTaskID = driver.getBindingDescriptor().outputGateBindings.get(gateIdx).get(channelIdx).taskID;;

            for(final MemoryView buffer : bufferStorage) {

                final IOEvents.TransferBufferEvent outputBuffer =
                        new IOEvents.TransferBufferEvent(driver.getNodeDescriptor().taskID, outputTaskID, buffer);

                emit(gateIdx, channelIdx, outputBuffer);
            }
        }

        done();

        driver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
    }*/

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void connectOutputDataChannels(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {
        // Connect outputs, if we have some...
        if (driver.getBindingDescriptor().outputGateBindings.size() > 0) {
            for (final List<Descriptors.AbstractNodeDescriptor> outputGate : outputBinding) {
                for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {
                    driver.connectDataChannel(outputTask, allocator);
                }
            }
        }
    }

    /**
     * Create the output gates from the binding descriptor.
     * 
     * @return The list of output gates or null if no the task has no outputs.
     */
    private void createOutputGates(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {

        outputGates.clear();

        if (outputBinding.size() <= 0) {

            driver.getTaskStateMachine().dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
            );

            return;
        }

        for (int gateIndex = 0; gateIndex < outputBinding.size(); ++gateIndex) {
            outputGates.add(new OutputGate(driver, gateIndex, this));
        }
    }

    /**
     * Create the mapping between task ID and corresponding gate index and between the channel index
     * and the corresponding task ID.
     */
    private void createOutputMappings(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {
        int channelIndex = 0;
        for (final List<Descriptors.AbstractNodeDescriptor> outputGate : outputBinding) {
            for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {
                taskIDToGateIndex.put(outputTask.taskID, channelIndex);
                channelIndexToTaskID.put(channelIndex, outputTask.taskID);
            }
            ++channelIndex;
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ProducerEventHandler extends EventHandler {

        @Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleTaskOutputDataChannelConnect(final IOEvents.DataIOEvent event) {
            int gateIndex = 0;
            boolean allOutputGatesConnected = true;
            for (final List<Descriptors.AbstractNodeDescriptor> outputGate : driver.getBindingDescriptor().outputGateBindings) {

                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;

                for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {

                    // Set the channel on right position.
                    if (outputTask.taskID.equals(event.dstTaskID)) {
                        // get the right queue manager for task context
                        final BufferQueue<IOEvents.DataIOEvent> queue = driver.getQueueManager().getOutboundQueue(gateIndex, channelIndex);

                        final DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.getPayload();
                        channelWriter.setOutboundQueue(queue);

                        final OutputGate og = outputGates.get(gateIndex);
                        og.setChannelWriter(channelIndex, channelWriter);

                        LOG.info("OUTPUT CONNECTION FROM " + driver.getNodeDescriptor().name + " [" + driver.getNodeDescriptor().taskID
                                + "] TO TASK " + outputTask.name + " [" + outputTask.taskID + "] IS ESTABLISHED");
                    }

                    // all data outputs are connected...
                    allOutputChannelsPerGateConnected &= (outputGates.get(gateIndex).getChannelWriter(channelIndex++) != null);
                }

                allOutputGatesConnected &= allOutputChannelsPerGateConnected;
                ++gateIndex;
            }

            if (allOutputGatesConnected) {
                LOG.debug("All output gates connected");
                driver.getTaskStateMachine().dispatchEvent(
                        new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
                );
            }
        }
    }
}
