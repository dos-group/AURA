package de.tuberlin.aura.taskmanager;

import java.util.*;

import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.MemoryView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BufferQueue;
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


    private final IAllocator outputAllocator;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataProducer(final ITaskDriver taskDriver, final IAllocator outputAllocator) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("taskDriver == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        this.taskDriver = taskDriver;

        this.outputAllocator = outputAllocator;

        // event handling.
        this.producerEventHandler = new ProducerEventHandler();

        this.taskDriver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, producerEventHandler);

        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();

        createOutputMappings();

        this.outputGates = createOutputGates();

        connectOutputDataChannels();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param gateIndex
     * @param channelIndex
     * @param event
     */
    public void emit(int gateIndex, int channelIndex, IOEvents.DataIOEvent event) {
        outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
    }

    /**
     *
     */
    public void done() {
        final List<Descriptors.TaskDescriptor> outputs = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);

        for (int index = 0; index < outputs.size(); ++index) {
            final UUID outputTaskID = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0).get(index).taskID;

            final IOEvents.DataIOEvent exhaustedEvent =
                    new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskDriver.getTaskDescriptor().taskID, outputTaskID);

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
        return outputAllocator.alloc();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void connectOutputDataChannels() {
        // Connect outputs, if we have some...
        if (taskDriver.getTaskBindingDescriptor().outputGateBindings.size() > 0) {
            for (final List<Descriptors.TaskDescriptor> outputGate : taskDriver.getTaskBindingDescriptor().outputGateBindings) {
                for (final Descriptors.TaskDescriptor outputTask : outputGate) {
                    taskDriver.connectDataChannel(outputTask, outputAllocator);
                }
            }
        }
    }

    /**
     * Create the output gates from the binding descriptor.
     * 
     * @return The list of output gates or null if no the task has no outputs.
     */
    private List<OutputGate> createOutputGates() {

        if (taskDriver.getTaskBindingDescriptor().outputGateBindings.size() <= 0) {

            taskDriver.getTaskStateMachine().dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
            );

            return null;
        }

        final List<OutputGate> outputGates = new ArrayList<>(taskDriver.getTaskBindingDescriptor().outputGateBindings.size());
        for (int gateIndex = 0; gateIndex < taskDriver.getTaskBindingDescriptor().outputGateBindings.size(); ++gateIndex) {
            outputGates.add(new OutputGate(taskDriver, gateIndex, this));
        }

        return outputGates;
    }

    /**
     * Create the mapping between task ID and corresponding gate index and between the channel index
     * and the corresponding task ID.
     */
    private void createOutputMappings() {
        int channelIndex = 0;
        for (final List<Descriptors.TaskDescriptor> outputGate : taskDriver.getTaskBindingDescriptor().outputGateBindings) {
            for (final Descriptors.TaskDescriptor outputTask : outputGate) {
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

        @Handle(event = IOEvents.GenericIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleTaskOutputDataChannelConnect(final IOEvents.GenericIOEvent event) {
            int gateIndex = 0;
            boolean allOutputGatesConnected = true;
            for (final List<Descriptors.TaskDescriptor> outputGate : taskDriver.getTaskBindingDescriptor().outputGateBindings) {

                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;

                for (final Descriptors.TaskDescriptor outputTask : outputGate) {

                    // Set the channel on right position.
                    if (outputTask.taskID.equals(event.dstTaskID)) {
                        // get the right queue manager for task context
                        final BufferQueue<IOEvents.DataIOEvent> queue = taskDriver.getQueueManager().getOutputQueue(gateIndex, channelIndex);

                        final DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.payload;
                        channelWriter.setOutputQueue(queue);

                        final OutputGate og = outputGates.get(gateIndex);
                        og.setChannelWriter(channelIndex, channelWriter);

                        LOG.info("OUTPUT CONNECTION FROM " + taskDriver.getTaskDescriptor().name + " [" + taskDriver.getTaskDescriptor().taskID
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
