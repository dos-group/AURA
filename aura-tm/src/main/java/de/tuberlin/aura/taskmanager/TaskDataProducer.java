package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.gates.OutputGate;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *
 */
public final class TaskDataProducer implements DataProducer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskDataProducer.class);

    private final TaskDriverContext driverContext;

    private final List<OutputGate> outputGates;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private final IEventHandler producerEventHandler;


    private final MemoryManager.Allocator allocator;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataProducer(final TaskDriverContext driverContext,
                            final MemoryManager.Allocator allocator) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");

        this.driverContext = driverContext;

        this.allocator = allocator;

        // event handling.
        this.producerEventHandler = new ProducerEventHandler();

        driverContext.driverDispatcher.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, producerEventHandler);

        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();

        createOutputMappings();

        connectOutputDataChannels();

        this.outputGates = createOutputGates();
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
        final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);

        for (int index = 0; index < outputs.size(); ++index) {
            final UUID outputTaskID = driverContext.taskBindingDescriptor.outputGateBindings.get(0).get(index).taskID;

            final IOEvents.DataIOEvent exhaustedEvent =
                    new IOEvents.DataIOEvent(
                            IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED,
                            driverContext.taskDescriptor.taskID,
                            outputTaskID
                    );

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

        //taskIDToGateIndex.clear();
        //channelIndexToTaskID.clear();
        //dispatcher.removeAllEventListener(); // TODO:
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
    public MemoryManager.MemoryView alloc() {
        return allocator.alloc();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void connectOutputDataChannels() {
        // Connect outputs, if we have some...
        if (driverContext.taskBindingDescriptor.outputGateBindings.size() > 0) {
            for (final List<Descriptors.TaskDescriptor> outputGate : driverContext.taskBindingDescriptor.outputGateBindings) {
                for (final Descriptors.TaskDescriptor outputTask : outputGate) {

                    driverContext.managerContext.ioManager.connectDataChannel(
                            driverContext.taskDescriptor.taskID,
                            outputTask.taskID,
                            outputTask.getMachineDescriptor(),
                            allocator
                    );
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

        if (driverContext.taskBindingDescriptor.outputGateBindings.size() <= 0) {

            driverContext.taskFSM.dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(
                            TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED
                    )
            );

            return null;
        }

        final List<OutputGate> outputGates = new ArrayList<>(driverContext.taskBindingDescriptor.outputGateBindings.size());
        for (int gateIndex = 0; gateIndex < driverContext.taskBindingDescriptor.outputGateBindings.size(); ++gateIndex) {
            outputGates.add(new OutputGate(driverContext, gateIndex, this));
        }

        return outputGates;
    }

    /**
     * Create the mapping between task ID and corresponding gate index and
     * between the channel index and the corresponding task ID.
     */
    private void createOutputMappings() {
        int channelIndex = 0;
        for (final List<Descriptors.TaskDescriptor> outputGate : driverContext.taskBindingDescriptor.outputGateBindings) {
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
            for (final List<Descriptors.TaskDescriptor> outputGate : driverContext.taskBindingDescriptor.outputGateBindings) {

                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;

                for (final Descriptors.TaskDescriptor outputTask : outputGate) {

                    // Set the channel on right position.
                    if (outputTask.taskID.equals(event.dstTaskID)) {
                        // get the right queue manager for task context
                        final BufferQueue<IOEvents.DataIOEvent> queue = driverContext.queueManager.getOutputQueue(gateIndex, channelIndex);

                        final DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.payload;
                        channelWriter.setOutputQueue(queue);

                        final OutputGate og = outputGates.get(gateIndex);
                        og.setChannelWriter(channelIndex, channelWriter);   // TODO: Sometimes NullPointerException.

                        LOG.info("OUTPUT CONNECTION FROM " + driverContext.taskDescriptor.name + " [" +
                                driverContext.taskDescriptor.taskID + "] TO TASK " + outputTask.name + " ["
                                + outputTask.taskID + "] IS ESTABLISHED");
                    }

                    // all data outputs are connected...
                    allOutputChannelsPerGateConnected &= (outputGates.get(gateIndex).getChannelWriter(channelIndex++) != null);
                }

                allOutputGatesConnected &= allOutputChannelsPerGateConnected;
                ++gateIndex;
            }

            if (allOutputGatesConnected) {
                driverContext.taskFSM.dispatchEvent(
                        new StateMachine.FSMTransitionEvent<>(
                                TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED
                        )
                );
            }
        }
    }
}
