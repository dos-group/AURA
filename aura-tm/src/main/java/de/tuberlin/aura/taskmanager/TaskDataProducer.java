package de.tuberlin.aura.taskmanager;

import java.util.*;

import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.storage.DataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.gates.OutputGate;

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

    private final ITaskDriver taskDriver;

    private final List<OutputGate> outputGates;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private final IEventHandler producerEventHandler;

    private IAllocator allocator;


    //private final List<MemoryView> bufferStorage;

    private List<List<Descriptors.AbstractNodeDescriptor>> outputBinding;

    private DataStorage dataStorage = null;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataProducer(final ITaskDriver taskDriver) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("driver == null");

        this.taskDriver = taskDriver;

        // event handling.
        this.producerEventHandler = new ProducerEventHandler();

        this.taskDriver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, producerEventHandler);

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
            dataStorage = new DataStorage(taskDriver, this, taskDriver.getDataConsumer(), taskDriver.getLOG());
        }
    }

    /**
     * @param gateIndex
     * @param channelIndex
     * @param event
     */
    public void emit(int gateIndex, int channelIndex, IOEvents.DataIOEvent event) {

        if(outputBinding.size() == 0)
            throw new IllegalStateException("no output binding");

        outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
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
        final List<Descriptors.AbstractNodeDescriptor> outputs = taskDriver.getBindingDescriptor().outputGateBindings.get(0);

        for (int index = 0; index < outputs.size(); ++index) {
            final UUID outputTaskID = taskDriver.getBindingDescriptor().outputGateBindings.get(0).get(index).taskID;

            final IOEvents.DataIOEvent exhaustedEvent =
                    new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskDriver.getNodeDescriptor().taskID, outputTaskID);

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
     * @return
     */
    @Override
    public MemoryView alloc() {
        return allocator.alloc();
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

        final List<Descriptors.AbstractNodeDescriptor> outputs = taskDriver.getBindingDescriptor().outputGateBindings.get(gateIdx);
        for (int channelIdx = 0; channelIdx < outputs.size(); ++channelIdx) {
            final UUID outputTaskID = taskDriver.getBindingDescriptor().outputGateBindings.get(gateIdx).get(channelIdx).taskID;;

            for(final MemoryView buffer : bufferStorage) {

                final IOEvents.TransferBufferEvent outputBuffer =
                        new IOEvents.TransferBufferEvent(taskDriver.getNodeDescriptor().taskID, outputTaskID, buffer);

                emit(gateIdx, channelIdx, outputBuffer);
            }
        }

        done();

        taskDriver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
    }*/

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void connectOutputDataChannels(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {
        // Connect outputs, if we have some...
        if (taskDriver.getBindingDescriptor().outputGateBindings.size() > 0) {
            for (final List<Descriptors.AbstractNodeDescriptor> outputGate : outputBinding) {
                for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {
                    taskDriver.connectDataChannel(outputTask, allocator);
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

            taskDriver.getTaskStateMachine().dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
            );

            return;
        }

        for (int gateIndex = 0; gateIndex < outputBinding.size(); ++gateIndex) {
            outputGates.add(new OutputGate(taskDriver, gateIndex, this));
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
            for (final List<Descriptors.AbstractNodeDescriptor> outputGate : taskDriver.getBindingDescriptor().outputGateBindings) {

                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;

                for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {

                    // Set the channel on right position.
                    if (outputTask.taskID.equals(event.dstTaskID)) {
                        // get the right queue manager for task context
                        final BufferQueue<IOEvents.DataIOEvent> queue = taskDriver.getQueueManager().getOutboundQueue(gateIndex, channelIndex);

                        final DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.getPayload();
                        channelWriter.setOutboundQueue(queue);

                        final OutputGate og = outputGates.get(gateIndex);
                        og.setChannelWriter(channelIndex, channelWriter);

                        LOG.info("OUTPUT CONNECTION FROM " + taskDriver.getNodeDescriptor().name + " [" + taskDriver.getNodeDescriptor().taskID
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
                taskDriver.getTaskStateMachine().dispatchEvent(
                        new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
                );
            }
        }
    }
}
