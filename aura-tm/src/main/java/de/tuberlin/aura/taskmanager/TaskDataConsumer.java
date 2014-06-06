package de.tuberlin.aura.taskmanager;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.gates.InputGate;

/**
 * The TaskDataConsumer is responsible for receiving data from connected TaskDataProducers and
 * provide this data (in form of buffers) to the task.
 */
public final class TaskDataConsumer implements IDataConsumer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskDataConsumer.class);

    private final ITaskDriver taskDriver;

    private final IEventHandler consumerEventHandler;


    private final List<InputGate> inputGates;

    private final List<Set<UUID>> activeGates;

    private final List<Map<UUID, Boolean>> closedGates;

    private final List<AtomicBoolean> gateCloseFinished;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;


    private boolean areInputsGatesExhausted;

    private IAllocator allocator;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataConsumer(final ITaskDriver taskDriver) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("driver == null");

        this.taskDriver = taskDriver;

        // event handling.
        this.consumerEventHandler = new ConsumerEventHandler();

        taskDriver.addEventListener(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, consumerEventHandler);

        this.inputGates = new ArrayList<>();

        // create mappings for input gates.
        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();

        // setup input gates.
        this.activeGates = new ArrayList<>();

        this.closedGates = new ArrayList<>();

        this.gateCloseFinished = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IOEvents.TransferBufferEvent absorb(int gateIndex) throws InterruptedException {

        if (activeGates.get(gateIndex).size() == 0)
            return null;

        boolean retrieve = true;

        IOEvents.DataIOEvent event = null;

        while (retrieve) {
            event = inputGates.get(gateIndex).getInputQueue().take();

            // DEBUGGING!
            /*
             * event = inputGates.get(gateIndex).getInputQueue().poll(20, TimeUnit.SECONDS);
             * if(event == null) { LOG.info("TIMEOUT"); LOG.info("GATE 0: size = " +
             * inputGates.get(0).getInputQueue().size()); LOG.info("GATE 1: size = " +
             * inputGates.get(1).getInputQueue().size()); }
             */

            switch (event.type) {

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                    final Set<UUID> activeChannelSet = activeGates.get(gateIndex);

                    if (!activeChannelSet.remove(event.srcTaskID))
                        throw new IllegalStateException();

                    if (activeChannelSet.isEmpty()) {
                        retrieve = false;
                        event = null;
                    }

                    // check if all gates are exhausted
                    boolean isExhausted = true;
                    for (final Set<UUID> acs : activeGates) {
                        isExhausted &= acs.isEmpty();
                    }
                    areInputsGatesExhausted = isExhausted;

                    break;

                case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK:
                    final Map<UUID, Boolean> closedChannels = closedGates.get(gateIndex);

                    if (!closedChannels.containsKey(event.srcTaskID))
                        throw new IllegalStateException();

                    closedChannels.put(event.srcTaskID, true);

                    boolean allClosed = true;
                    for (boolean closed : closedChannels.values()) {
                        allClosed &= closed;
                    }

                    if (allClosed) {
                        gateCloseFinished.get(gateIndex).set(true);
                        retrieve = false;
                        event = null;
                    }
                    break;

                // data event -> absorb
                default:
                    retrieve = false;
                    break;
            }
        }

        return (IOEvents.TransferBufferEvent) event;
    }

    /**
     *
     */
    public void shutdownConsumer() {
        // taskIDToGateIndex.clear();
        // channelIndexToTaskID.clear();
    }

    /**
     * @param gateIndex
     */
    public void openGate(int gateIndex) {
        if (inputGates == null) {
            throw new IllegalStateException("Task has no input gates.");
        }
        inputGates.get(gateIndex).openGate();
    }

    /**
     * @param gateIndex
     */
    public void closeGate(final int gateIndex) {
        if (inputGates == null) {
            throw new IllegalStateException("Task has no input gates.");
        }

        gateCloseFinished.get(gateIndex).set(false);
        inputGates.get(gateIndex).closeGate();
    }

    /**
     * @param gateIndex
     * @return
     */
    public boolean isGateClosed(int gateIndex) {
        return gateCloseFinished.get(gateIndex).get();
    }

    /**
     * @param channelIndex
     * @return
     */
    public UUID getInputTaskIDFromChannelIndex(int channelIndex) {
        return channelIndexToTaskID.get(channelIndex);
    }

    /**
     * Return the gate index for the corresponding task ID.
     * 
     * @param taskID The unique ID of a connected task.
     * @return The gate index or null if no suitable mapping exists.
     */
    public int getInputGateIndexFromTaskID(final UUID taskID) {
        return taskIDToGateIndex.get(taskID);
    }

    /**
     * @return
     */
    public boolean isExhausted() {
        return areInputsGatesExhausted;
    }

    /**
     *
     */
    @Override
    public void bind(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding, final IAllocator allocator) {
        // sanity check.
        if(inputBinding == null)
            throw new IllegalArgumentException("inputBinding == null");
        if(allocator == null)
            throw new IllegalArgumentException("allocator == null");

        this.allocator = allocator;

        // create mappings for input gates.
        this.taskIDToGateIndex.clear();

        this.channelIndexToTaskID.clear();

        createInputMappings(inputBinding);

        // create input gates.
        createInputGates(inputBinding);

        // setup input gates.
        this.activeGates.clear();

        this.closedGates.clear();

        this.gateCloseFinished.clear();

        this.areInputsGatesExhausted = false;

        setupInputGates(inputBinding);
    }

    /**
     *
     * @return
     */
    @Override
    public IAllocator getAllocator() {
        return allocator;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * Create the output gates from the binding descriptor.
     * 
     * @return The list of output gates or null if no the task has no outputs.
     */
    private void createInputGates(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {

        this.inputGates.clear();

        if (inputBinding.size() <= 0) {

            taskDriver.getTaskStateMachine().dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED)
            );
            return;
        }

        for (int gateIndex = 0; gateIndex < inputBinding.size(); ++gateIndex) {
            inputGates.add(new InputGate(taskDriver, gateIndex));
        }
    }

    /**
     *
     */
    private void setupInputGates(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {
        for (final List<Descriptors.AbstractNodeDescriptor> tdList : inputBinding) {

            final Map<UUID, Boolean> closedChannels = new HashMap<>();
            closedGates.add(closedChannels);

            final Set<UUID> activeChannelSet = new HashSet<>();
            activeGates.add(activeChannelSet);

            gateCloseFinished.add(new AtomicBoolean(false));

            for (final Descriptors.AbstractNodeDescriptor td : tdList) {
                closedChannels.put(td.taskID, false);
                activeChannelSet.add(td.taskID);
            }
        }
    }

    /**
     * Create the mapping between task ID and corresponding gate index and the between channel index
     * and the corresponding task ID.
     */
    private void createInputMappings(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {
        int channelIndex = 0;
        for (final List<Descriptors.AbstractNodeDescriptor> inputGate : inputBinding) {
            for (final Descriptors.AbstractNodeDescriptor inputTask : inputGate) {
                taskIDToGateIndex.put(inputTask.taskID, channelIndex);
                channelIndexToTaskID.put(channelIndex, inputTask.taskID);
            }
            ++channelIndex;
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ConsumerEventHandler extends EventHandler {

        @Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleTaskInputDataChannelConnect(final IOEvents.DataIOEvent event) {

            int gateIndex = 0;
            boolean allInputGatesConnected = true, connectingToCorrectTask = false;

            for (final List<Descriptors.AbstractNodeDescriptor> inputGate : taskDriver.getBindingDescriptor().inputGateBindings) {

                int channelIndex = 0;
                boolean allInputChannelsPerGateConnected = true;

                for (final Descriptors.AbstractNodeDescriptor inputTask : inputGate) {

                    // get the right input gate for src the event comes from.
                    if (inputTask.taskID.equals(event.srcTaskID)) {

                        // wire queue to input gate
                        final DataReader channelReader = (DataReader) event.getPayload();

                        // create queue, if there is none yet as we can have multiple channels
                        // insert in one queue (aka multiple channels per gate)
                        final BufferQueue<IOEvents.DataIOEvent> queue = taskDriver.getQueueManager().getInboundQueue(gateIndex);
                        channelReader.bindQueue(taskDriver.getNodeDescriptor().taskID, event.getChannel(), gateIndex, channelIndex, queue);

                        inputGates.get(gateIndex).setChannelReader(channelReader);

                        LOG.info("INPUT CONNECTION FROM " + inputTask.name + " [" + inputTask.taskID + "] TO TASK "
                                + taskDriver.getNodeDescriptor().name + " [" + taskDriver.getNodeDescriptor().taskID + "] IS ESTABLISHED");

                        connectingToCorrectTask |= true;
                    }

                    boolean connected = false;
                    if (inputGates.get(gateIndex).getChannelReader() != null) {
                        connected =
                                inputGates.get(gateIndex)
                                          .getChannelReader()
                                          .isConnected(taskDriver.getNodeDescriptor().taskID, gateIndex, channelIndex);
                    }

                    // all data inputs are connected...
                    allInputChannelsPerGateConnected &= connected;
                    channelIndex++;
                }

                allInputGatesConnected &= allInputChannelsPerGateConnected;
                ++gateIndex;
            }

            // Check if the incoming channel is connecting to the correct task.
            if (!connectingToCorrectTask) {
                throw new IllegalStateException("wrong data channel tries to connect");
            }

            if (allInputGatesConnected) {
                taskDriver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
            }
        }
    }
}