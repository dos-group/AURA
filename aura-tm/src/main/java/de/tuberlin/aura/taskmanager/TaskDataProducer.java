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
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.gates.OutputGate;
import de.tuberlin.aura.core.taskmanager.spi.IDataProducer;
import de.tuberlin.aura.core.taskmanager.spi.ITaskDriver;


public final class TaskDataProducer implements IDataProducer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskDataProducer.class);

    private final ITaskDriver driver;

    private final List<OutputGate> outputGates;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<UUID, Integer> taskIDToChannelIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private IAllocator allocator;

    private List<List<Descriptors.AbstractNodeDescriptor>> outputBinding;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataProducer(final ITaskDriver driver) {
        // Sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");

        this.driver = driver;

        final IEventHandler producerEventHandler = new ProducerEventHandler();

        this.driver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, producerEventHandler);

        this.outputGates = new ArrayList<>();

        this.taskIDToGateIndex = new HashMap<>();

        this.taskIDToChannelIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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
        this.taskIDToChannelIndex.clear();
        this.channelIndexToTaskID.clear();

        createOutputMappings(outputBinding);
        createOutputGates(outputBinding);
        connectOutputDataChannels(outputBinding);
    }

    public void emit(final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");
        if (outputBinding.size() == 0)
            throw new IllegalStateException("no output binding");

        outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
    }

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

    public void done(final int outputGateIndex) {
        final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(outputGateIndex);

        for (int index = 0; index < outputs.size(); ++index) {
            final UUID outputTaskID = driver.getBindingDescriptor().outputGateBindings.get(outputGateIndex).get(index).taskID;
            final IOEvents.DataIOEvent exhaustedEvent =
                    new IOEvents.DataIOEvent(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, driver.getNodeDescriptor().taskID, outputTaskID);
            emit(outputGateIndex, index, exhaustedEvent);
        }
    }

    public void shutdownProducer(boolean awaitExhaustion) {
        for (final OutputGate og : outputGates) {
            // TODO: maybe replace with event?!
            for (final DataWriter.ChannelWriter channelWriter : og.getAllChannelWriter()) {
                channelWriter.shutdown(awaitExhaustion);
            }
        }

        // taskIDToGateIndex.clear();
        // channelIndexToTaskID.clear();
        // dispatcher.removeAllEventListener(); // TODO:
    }

    public UUID getOutputTaskIDFromChannelIndex(int channelIndex) {
        return channelIndexToTaskID.get(channelIndex);
    }

    /**
     * Return the gate index for the corresponding taskmanager ID.
     * 
     * @param taskID The unique ID of a connected taskmanager.
     * @return The gate index or null if no suitable mapping exists.
     */
    public int getOutputGateIndexFromTaskID(final UUID taskID) {
        return taskIDToGateIndex.get(taskID);
    }

    public int getChannelIndexFromTaskID(final UUID taskID) {
        return taskIDToChannelIndex.get(taskID);
    }

    @Override
    public IAllocator getAllocator() {
        return allocator;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

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
     * Create the mapping between taskmanager ID and corresponding gate index and between the channel index
     * and the corresponding taskmanager ID.
     */
    private void createOutputMappings(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {

        remainingChannelsToConnect = new int[outputBinding.size()];

        int gateIndex = 0;
        for (final List<Descriptors.AbstractNodeDescriptor> outputGate : outputBinding) {
            int channelIndex = 0;
            for (final Descriptors.AbstractNodeDescriptor outputTask : outputGate) {
                taskIDToGateIndex.put(outputTask.taskID, gateIndex);
                channelIndexToTaskID.put(gateIndex, outputTask.taskID);
                taskIDToChannelIndex.put(outputTask.taskID, channelIndex);
                channelIndex++;
            }
            remainingChannelsToConnect[gateIndex] = outputGate.size();
            ++gateIndex;
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private int[] remainingChannelsToConnect;

    private final class ProducerEventHandler extends EventHandler {

        @Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleTaskOutputDataChannelConnect(final IOEvents.DataIOEvent event) {

            try {

                int gateIndex = taskIDToGateIndex.get(event.dstTaskID);
                int channelIndex = taskIDToChannelIndex.get(event.dstTaskID);
                // get the right queue manager for taskmanager context
                final BufferQueue<IOEvents.DataIOEvent> queue = driver.getQueueManager().getOutboundQueue(gateIndex, channelIndex);
                final DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.getPayload();
                channelWriter.setOutboundQueue(queue);
                final OutputGate og = outputGates.get(gateIndex);
                og.setChannelWriter(channelIndex, channelWriter);
                Descriptors.AbstractNodeDescriptor dst = driver.getBindingDescriptor().outputGateBindings.get(gateIndex).get(channelIndex);

                LOG.debug("OUTPUT CONNECTION FROM " + driver.getNodeDescriptor().name + " [" + driver.getNodeDescriptor().taskID
                        + "] TO TASK " + dst.name + " [" + dst.taskID + "] IS ESTABLISHED");

                remainingChannelsToConnect[gateIndex]--;
                if (remainingChannelsToConnect[gateIndex] < 0) {
                    throw new IllegalStateException("unexpected channel connected to gate: " + gateIndex);
                }

                // if all channels for gate are connected
                if (remainingChannelsToConnect[gateIndex] == 0) {
                    // if all gates are fully connected
                    boolean fullyConnected = true;
                    for (int remaining: remainingChannelsToConnect) {
                        fullyConnected &= remaining == 0;
                    }
                    if (fullyConnected) {
                        LOG.debug("All output gates connected");
                        driver.getTaskStateMachine().dispatchEvent(
                                new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED)
                        );
                    }
                }
            } catch (Exception ex) {
                throw new IllegalStateException("unexpected channel tried to connect");
            }
        }
    }
}
