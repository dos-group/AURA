package de.tuberlin.aura.taskmanager;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.IAllocator;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.gates.InputGate;

/**
 * The TaskDataConsumer is responsible for receiving data from connected TaskDataProducers and
 * provide this data (in form of buffers) to the task.
 */
public final class TaskDataConsumer implements DataConsumer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskDataConsumer.class);

    private final TaskDriverContext driverContext;

    private final List<InputGate> inputGates;

    private final List<Set<UUID>> activeGates;

    private final List<Map<UUID, Boolean>> closedGates;

    private final List<AtomicBoolean> gateCloseFinished;

    private boolean areInputsGatesExhausted;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Integer, UUID> channelIndexToTaskID;

    private final IEventHandler consumerEventHandler;

    private final IAllocator allocator;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDataConsumer(final TaskDriverContext driverContext, final IAllocator allocator) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");

        this.driverContext = driverContext;

        this.allocator = allocator;

        // event handling.
        this.consumerEventHandler = new ConsumerEventHandler();

        driverContext.driverDispatcher.addEventListener(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, consumerEventHandler);

        // create mappings for input gates.
        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToTaskID = new HashMap<>();

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

    public IOEvents.TransferBufferEvent absorb(int gateIndex) throws InterruptedException {

        if (activeGates.get(gateIndex).size() == 0)
            return null;

        boolean retrieve = true;

        IOEvents.DataIOEvent event = null;

        while (retrieve) {
            event = inputGates.get(gateIndex).getInputQueue().take();

            // DEBUGGING!
            /*
             * event = inputGates.get(gateIndex).getInboundQueue().poll(20, TimeUnit.SECONDS);
             * if(event == null) { LOG.info("TIMEOUT"); LOG.info("GATE 0: size = " +
             * inputGates.get(0).getInboundQueue().size()); LOG.info("GATE 1: size = " +
             * inputGates.get(1).getInboundQueue().size()); }
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

            driverContext.taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));

            return null;
        }

        final List<InputGate> inputGates = new ArrayList<>(driverContext.taskBindingDescriptor.inputGateBindings.size());
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

            final Map<UUID, Boolean> closedChannels = new HashMap<>();
            closedGates.add(closedChannels);

            final Set<UUID> activeChannelSet = new HashSet<>();
            activeGates.add(activeChannelSet);

            gateCloseFinished.add(new AtomicBoolean(false));

            for (final Descriptors.TaskDescriptor td : tdList) {
                closedChannels.put(td.taskID, false);
                activeChannelSet.add(td.taskID);
            }
        }
    }

    /**
     * Create the mapping between task ID and corresponding gate index and the between channel index
     * and the corresponding task ID.
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

            boolean found = false;

            int gateIndex = 0;
            boolean allInputGatesConnected = true, connectingToCorrectTask = false;

            for (final List<Descriptors.TaskDescriptor> inputGate : driverContext.taskBindingDescriptor.inputGateBindings) {

                int channelIndex = 0;
                boolean allInputChannelsPerGateConnected = true;

                for (final Descriptors.TaskDescriptor inputTask : inputGate) {

                    // get the right input gate for src the event comes from.
                    if (inputTask.taskID.equals(event.srcTaskID)) {

                        // wire queue to input gate
                        final DataReader channelReader = (DataReader) event.payload;

                        // create queue, if there is none yet as we can have multiple channels
                        // insert in one queue (aka multiple channels per gate)
                        final BufferQueue<IOEvents.DataIOEvent> queue = driverContext.queueManager.getInboundQueue(gateIndex);
                        channelReader.bindQueue(driverContext.taskDescriptor.taskID, event.getChannel(), gateIndex, channelIndex, queue);

                        inputGates.get(gateIndex).setChannelReader(channelReader);

                        LOG.debug("INPUT CONNECTION FROM " + inputTask.name + " [" + inputTask.taskID + "] TO TASK "
                                + driverContext.taskDescriptor.name + " [" + driverContext.taskDescriptor.taskID + "] IS ESTABLISHED");

                        found = true;

                        connectingToCorrectTask |= true;
                    }

                    boolean connected = false;
                    if (inputGates.get(gateIndex).getChannelReader() != null) {
                        connected =
                                inputGates.get(gateIndex)
                                          .getChannelReader()
                                          .isConnected(driverContext.taskDescriptor.taskID, gateIndex, channelIndex);
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
                driverContext.taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
            }
        }
    }
}

// ----------------------------------------------------

// private boolean initialAbsorbCall = true;

// private IOEvents.DataIOEvent eventLookUp = null;

/**
 * 
 * @param gateIndex
 * @return
 * @throws InterruptedException
 */
/*
 * public IOEvents.TransferBufferEvent absorb(int gateIndex) throws InterruptedException {
 * 
 * final IOEvents.DataIOEvent event;
 * 
 * if (initialAbsorbCall) {
 * 
 * event = inputGates.get(gateIndex).getInboundQueue().take();
 * 
 * // TODO: What happens if DATA_EVENT_SOURCE_EXHAUSTED occurs in the first call ?
 * 
 * initialAbsorbCall = false;
 * 
 * } else { event = eventLookUp; }
 * 
 * boolean retrieve = true;
 * 
 * while (retrieve) {
 * 
 * eventLookUp = inputGates.get(gateIndex).getInboundQueue().take();
 * 
 * switch (eventLookUp.type) {
 * 
 * case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED: { final Set<UUID> activeChannelSet =
 * activeGates.get(gateIndex);
 * 
 * if (!activeChannelSet.remove(event.srcTaskID)) throw new IllegalStateException();
 * 
 * if (activeChannelSet.isEmpty()) { retrieve = false; eventLookUp = null; }
 * 
 * // check if all gates are exhausted boolean isExhausted = true; for (final Set<UUID> acs :
 * activeGates) { isExhausted &= acs.isEmpty(); } areInputsGatesExhausted = isExhausted;
 * 
 * } break;
 * 
 * case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_ACK: { final Map<UUID, Boolean>
 * closedChannels = closedGates.get(gateIndex);
 * 
 * if (!closedChannels.containsKey(event.srcTaskID)) throw new IllegalStateException();
 * 
 * closedChannels.put(event.srcTaskID, true);
 * 
 * boolean allClosed = true; for (boolean closed : closedChannels.values()) { allClosed &= closed; }
 * 
 * if (allClosed) { gateCloseFinished.get(gateIndex).set(true); retrieve = false; eventLookUp =
 * null;
 * 
 * areInputsGatesExhausted = true; // TODO: Should we block here? }
 * 
 * } break;
 * 
 * // data event -> absorb default: retrieve = false; break; } }
 * 
 * return (IOEvents.TransferBufferEvent) event; }
 */
