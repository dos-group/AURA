package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskStateMachine;
import de.tuberlin.aura.core.task.gates.InputGate;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The TaskDataConsumer is responsible for receiving data from connected TaskDataProducers
 * and provide this data (in form of buffers) to the task.
 */
public final class TaskDataConsumer implements DataConsumer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskDataConsumer.class);

    private final TaskDriverContext driverContext;

    private final List<InputGate> inputGates;

    private final List<Set<UUID>> activeGates;

    private final List<Map<UUID, Boolean>> closedGates;

    private final List<AtomicBoolean> gateCloseFinished;

    private boolean areInputsGatesExhausted;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private final IEventHandler consumerEventHandler;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataConsumer(final TaskDriverContext driverContext) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");

        this.driverContext = driverContext;

        // event handling.
        this.consumerEventHandler = new ConsumerEventHandler();

        driverContext.driverDispatcher.addEventListener(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, consumerEventHandler);

        // create mappings for input gates.
        this.taskIDToGateIndex = new HashMap<UUID, Integer>();

        this.channelIndexToTaskID = new HashMap<Integer, UUID>();

        createInputMappings();

        // create input gates.
        this.inputGates = createInputGates();

        // setup input gates.
        this.activeGates = new ArrayList<>();

        this.closedGates = new ArrayList<>();

        this.gateCloseFinished = new ArrayList<>();

        this.areInputsGatesExhausted = false;

        setupInputGates();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IOEvents.DataBufferEvent absorb(int gateIndex) throws InterruptedException {

        if (activeGates.get(gateIndex).size() == 0)
            return null;

        boolean retrieve = true;

        IOEvents.DataIOEvent event = null;

        while (retrieve) {

            event = inputGates.get(gateIndex).getInputQueue().take();

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

        return (IOEvents.DataBufferEvent) event;
    }

    /**
     *
     */
    public void shutdownConsumer() {
        //taskIDToGateIndex.clear();
        //channelIndexToTaskID.clear();
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

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * Create the output gates from the binding descriptor.
     *
     * @return The list of output gates or null if no the task has no outputs.
     */
    private List<InputGate> createInputGates() {

        if (driverContext.taskBindingDescriptor.inputGateBindings.size() <= 0) {

            driverContext.driverDispatcher.dispatchEvent(
                    new IOEvents.TaskStateTransitionEvent(
                            driverContext.taskDescriptor.topologyID,
                            driverContext.taskDescriptor.taskID,
                            TaskStateMachine.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED
                    )
            );

            return null;
        }

        final List<InputGate> inputGates = new ArrayList<InputGate>(driverContext.taskBindingDescriptor.inputGateBindings.size());
        for (int gateIndex = 0; gateIndex < driverContext.taskBindingDescriptor.inputGateBindings.size(); ++gateIndex) {
            inputGates.add(new InputGate(driverContext, gateIndex)); // TODO:
        }

        return inputGates;
    }

    /**
     *
     */
    private void setupInputGates() {
        for (final List<Descriptors.TaskDescriptor> tdList : driverContext.taskBindingDescriptor.inputGateBindings) {

            final Map<UUID, Boolean> closedChannels = new HashMap<UUID, Boolean>();
            closedGates.add(closedChannels);

            final Set<UUID> activeChannelSet = new HashSet<UUID>();
            activeGates.add(activeChannelSet);

            gateCloseFinished.add(new AtomicBoolean(false));

            for (final Descriptors.TaskDescriptor td : tdList) {
                closedChannels.put(td.taskID, false);
                activeChannelSet.add(td.taskID);
            }
        }
    }

    /**
     * Create the mapping between task ID and corresponding gate index and
     * the between channel index and the corresponding task ID.
     */
    private void createInputMappings() {
        int channelIndex = 0;
        for (final List<Descriptors.TaskDescriptor> inputGate : driverContext.taskBindingDescriptor.inputGateBindings) {
            for (final Descriptors.TaskDescriptor inputTask : inputGate) {
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

        @Handle(event = IOEvents.GenericIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleTaskInputDataChannelConnect(final IOEvents.GenericIOEvent event) {

            int gateIndex = 0;
            boolean allInputGatesConnected = true, connectingToCorrectTask = false;

            for (final List<Descriptors.TaskDescriptor> inputGate : driverContext.taskBindingDescriptor.inputGateBindings) {

                int channelIndex = 0;
                boolean allInputChannelsPerGateConnected = true;

                for (Descriptors.TaskDescriptor inputTask : inputGate) {

                    // get the right input gate for src the event comes from.
                    if (inputTask.taskID.equals(event.srcTaskID)) {

                        // wire queue to input gate
                        final DataReader channelReader = (DataReader) event.payload;

                        // create queue, if there is none yet as we can have multiple channels
                        // insert in one queue (aka multiple channels per gate)
                        BufferQueue<IOEvents.DataIOEvent> queue = driverContext.queueManager.getInputQueue(gateIndex);
                        channelReader.bindQueue(driverContext.taskDescriptor.taskID, event.getChannel(), gateIndex, channelIndex, queue);

                        inputGates.get(gateIndex).setChannelReader(channelReader);

                        LOG.info("INPUT CONNECTION FROM " + inputTask.name + " [" + inputTask.taskID + "] TO TASK "
                                + driverContext.taskDescriptor.name + " ["
                                + driverContext.taskDescriptor.taskID + "] IS ESTABLISHED");

                        connectingToCorrectTask |= true;
                    }

                    boolean connected = false;
                    if (inputGates.get(gateIndex).getChannelReader() != null) {
                        connected = inputGates.get(gateIndex).getChannelReader().isConnected(driverContext.taskDescriptor.taskID, gateIndex, channelIndex);
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
                driverContext.driverDispatcher.dispatchEvent(
                        new IOEvents.TaskStateTransitionEvent(
                                driverContext.taskDescriptor.topologyID,
                                driverContext.taskDescriptor.taskID,
                                TaskStateMachine.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED
                        )
                );
            }
        }
    }
}

//----------------------------------------------------

//private boolean initialAbsorbCall = true;

//private IOEvents.DataIOEvent eventLookUp = null;

/**
 *
 * @param gateIndex
 * @return
 * @throws InterruptedException
 */
    /*public IOEvents.DataBufferEvent absorb(int gateIndex) throws InterruptedException {

        final IOEvents.DataIOEvent event;

        if (initialAbsorbCall) {

            event = inputGates.get(gateIndex).getInputQueue().take();

            // TODO: What happens if DATA_EVENT_SOURCE_EXHAUSTED occurs in the first call ?

            initialAbsorbCall = false;

        } else {
            event = eventLookUp;
        }

        boolean retrieve = true;

        while (retrieve) {

            eventLookUp = inputGates.get(gateIndex).getInputQueue().take();

            switch (eventLookUp.type) {

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED: {
                    final Set<UUID> activeChannelSet = activeGates.get(gateIndex);

                    if (!activeChannelSet.remove(event.srcTaskID))
                        throw new IllegalStateException();

                    if (activeChannelSet.isEmpty()) {
                        retrieve = false;
                        eventLookUp = null;
                    }

                    // check if all gates are exhausted
                    boolean isExhausted = true;
                    for (final Set<UUID> acs : activeGates) {
                        isExhausted &= acs.isEmpty();
                    }
                    areInputsGatesExhausted = isExhausted;

                } break;

                case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK: {
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
                        eventLookUp = null;

                        areInputsGatesExhausted = true; // TODO: Should we block here?
                    }

                } break;

                // data event -> absorb
                default:
                    retrieve = false;
                break;
            }
        }

        return (IOEvents.DataBufferEvent) event;
    }*/