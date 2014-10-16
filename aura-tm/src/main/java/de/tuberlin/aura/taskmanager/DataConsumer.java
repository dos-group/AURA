package de.tuberlin.aura.taskmanager;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.gates.InputGate;
import de.tuberlin.aura.core.taskmanager.spi.IDataConsumer;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;


public final class DataConsumer implements IDataConsumer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(DataConsumer.class);

    private final ITaskRuntime runtime;

    private final List<InputGate> inputGates;

    private final List<Set<UUID>> activeGates;

    private final List<Map<UUID, Boolean>> closedGates;

    private final List<AtomicBoolean> gateCloseFinished;

    private final Map<UUID, Integer> taskIDToGateIndex;

    private final Map<Pair<Integer,Integer>, UUID> channelIndexToSenderTaskID;

    private boolean areInputsGatesExhausted;

    private IAllocator allocator;

    private final List<RoundRobinAbsorber> absorber;

    private final Map<UUID, Integer> senderTaskIDToChannelIndex = new HashMap<>();

    private int[] remainingChannelsToConnect;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DataConsumer(final ITaskRuntime runtime) {
        // sanity check.
        if (runtime == null)
            throw new IllegalArgumentException("runtime == null");

        this.runtime = runtime;

        final IEventHandler consumerEventHandler = new ConsumerEventHandler();

        runtime.addEventListener(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, consumerEventHandler);

        this.inputGates = new ArrayList<>();

        this.absorber = new ArrayList<>();

        this.taskIDToGateIndex = new HashMap<>();

        this.channelIndexToSenderTaskID = new HashMap<>();

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

            event = absorber.get(gateIndex).take();

            switch (event.type) {

                case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                    final Set<UUID> activeChannelSet = activeGates.get(gateIndex);

                    if (!activeChannelSet.remove(event.srcTaskID))
                        throw new IllegalStateException();

                    if (activeChannelSet.isEmpty()) {
                        retrieve = false;
                        event = null;
                        LOG.debug("all channels exhausted.");
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

    public IOEvents.TransferBufferEvent absorb(int gateIndex, int channelIndex) throws InterruptedException {

        if (activeGates.get(gateIndex).size() == 0)
            return null;

        boolean retrieve = true;

        IOEvents.DataIOEvent event = null;

        while (retrieve) {

            event = inputGates.get(gateIndex).getInputQueue(channelIndex).poll(20, TimeUnit.SECONDS);
            if (event == null) {
                for (int i =  0; i < inputGates.get(gateIndex).getNumOfChannels(); i++) {
                    LOG.warn(i + " queue: " + inputGates.get(gateIndex).getInputQueue(i).size());
                }
                event = inputGates.get(gateIndex).getInputQueue(channelIndex).take();
            }


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

    public void shutdown() {
        // taskIDToGateIndex.clear();
        // channelIndexToSenderTaskID.clear();
    }

    public void openGate(int gateIndex) {
        inputGates.get(gateIndex).openGate();
    }

    public void closeGate(final int gateIndex) {
        gateCloseFinished.get(gateIndex).set(false);
        inputGates.get(gateIndex).closeGate();
    }

    public boolean isGateClosed(final int gateIndex) {
        return gateCloseFinished.get(gateIndex).get();
    }

    public UUID getInputTaskIDFromChannelIndex(final int channelIndex) {
        return null; //return channelIndexToSenderTaskID.get(channelIndex); // TODO: that´s a wrong implementation, not channelIndex but gateIndex...
    }

    public int getChannelIndexFromTaskID(final UUID taskID) {
        return senderTaskIDToChannelIndex.get(taskID);
    }

    public int getInputGateIndexFromTaskID(final UUID taskID) {

        // see DataConsumer.getExecutionUnitByTaskID(..) for the reasoning behind this polling..

        Integer gateIndex = taskIDToGateIndex.get(taskID);

        for (int i=0; i < 20 && gateIndex == null; i++) {
            try {
                Thread.sleep(50);
                gateIndex = taskIDToGateIndex.get(taskID);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (gateIndex == null) {
            throw new IllegalStateException("Could not find gate for task");
        }

        return gateIndex;
    }

    public boolean isExhausted() {
        return areInputsGatesExhausted;
    }

    @Override
    public void bind(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding, final IAllocator allocator) {
        // sanity check.
        if(inputBinding == null)
            throw new IllegalArgumentException("inputBinding == null");
        if(allocator == null)
            throw new IllegalArgumentException("allocator == null");

        this.allocator = allocator;

        this.taskIDToGateIndex.clear();
        this.channelIndexToSenderTaskID.clear();
        createInputMappings(inputBinding);
        createInputGates(inputBinding);

        this.activeGates.clear();
        this.closedGates.clear();
        this.gateCloseFinished.clear();
        this.areInputsGatesExhausted = false;
        setupInputGates(inputBinding);
    }

    @Override
    public IAllocator getAllocator() {
        return allocator;
    }

    public boolean isInputChannelExhausted(final int gateIndex, final UUID srcTaskID) {
        // sanity check.
        if (srcTaskID == null)
            throw new IllegalArgumentException("srcTaskID == null");
        if(gateIndex < 0 || gateIndex >= runtime.getBindingDescriptor().inputGateBindings.get(gateIndex).size())
            throw new IndexOutOfBoundsException();

        return !activeGates.get(gateIndex).contains(srcTaskID);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void createInputGates(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {
        this.inputGates.clear();
        if (inputBinding.size() <= 0) {
            runtime.getTaskStateMachine().dispatchEvent(
                    new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED)
            );
            return;
        }
        for (int gateIndex = 0; gateIndex < inputBinding.size(); ++gateIndex) {
            final InputGate inputGate = new InputGate(runtime, gateIndex);
            inputGates.add(inputGate);
            final RoundRobinAbsorber rra = new RoundRobinAbsorber(inputGate);
            absorber.add(rra);
        }
    }

    private void setupInputGates(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {
        remainingChannelsToConnect = new int[inputBinding.size()];
        int gateIndex = 0;
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
            remainingChannelsToConnect[gateIndex++] = tdList.size();
        }
    }

    private void createInputMappings(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding) {
        int gateIndex = 0;
        for (final List<Descriptors.AbstractNodeDescriptor> inputGate : inputBinding) {
            int channelIndex = 0;
            for (final Descriptors.AbstractNodeDescriptor inputTask : inputGate) {
                taskIDToGateIndex.put(inputTask.taskID, gateIndex);
                channelIndexToSenderTaskID.put(new Pair<>(gateIndex, channelIndex), inputTask.taskID);
                senderTaskIDToChannelIndex.put(inputTask.taskID, channelIndex);
                ++channelIndex;
            }
            ++gateIndex;
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ConsumerEventHandler extends EventHandler {

        @Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleTaskInputDataChannelConnect(final IOEvents.DataIOEvent event) {

            try {
                int gateIndex = taskIDToGateIndex.get(event.srcTaskID);
                int channelIndex = senderTaskIDToChannelIndex.get(event.srcTaskID);
                final DataReader channelReader = (DataReader) event.getPayload();
                final BufferQueue<IOEvents.DataIOEvent> queue = runtime.getQueueManager().getInboundQueue(gateIndex, channelIndex);
                channelReader.bindQueue(runtime.getNodeDescriptor().taskID, event.getChannel(), gateIndex, channelIndex, queue);
                inputGates.get(gateIndex).setDataReader(channelReader);
                final Descriptors.AbstractNodeDescriptor src = runtime.getBindingDescriptor().inputGateBindings.get(gateIndex).get(channelIndex);

                LOG.debug("INPUT CONNECTION FROM " + src.name + " [" + src.taskID + "] TO TASK "
                        + runtime.getNodeDescriptor().name + " [" + runtime.getNodeDescriptor().taskID + "] IS ESTABLISHED");

                remainingChannelsToConnect[gateIndex]--;
                if (remainingChannelsToConnect[gateIndex] < 0) {
                    throw new IllegalStateException("unexpected channel connected to gate: " + gateIndex);
                }

                if (remainingChannelsToConnect[gateIndex] == 0) {
                    absorber.get(gateIndex).initQueues();
                    boolean fullyConnected = true;
                    for (int remaining: remainingChannelsToConnect)
                        fullyConnected &= remaining == 0;
                    if (fullyConnected) {
                        runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
                    }
                }

            } catch (Exception ex) {
                throw new IllegalStateException("unexpected channel tried to connect");
            }
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class RoundRobinAbsorber {

        private final InputGate inputGate;

        private final BlockingQueue<BufferQueue<IOEvents.DataIOEvent>> absorberQueue = new LinkedBlockingQueue<>();

        public RoundRobinAbsorber(final InputGate inputGate) {
            // sanity check.
            if (inputGate == null)
                throw new IllegalArgumentException("inputGate == null");

            this.inputGate = inputGate;
        }

        public void initQueues() {

            for (int channelIndex = 0; channelIndex < inputGate.getNumOfChannels(); ++channelIndex) {

                final BufferQueue<IOEvents.DataIOEvent> bufferQueue = inputGate.getInputQueue(channelIndex);

                final BufferQueue.QueueObserver qo = new BufferQueue.QueueObserver() {

                    @Override
                    public void signalNotEmpty() {
                        absorberQueue.add(bufferQueue);
                    }

                    @Override
                    public void signalNotFull() {}

                    @Override
                    public void signalNewElement() {}
                };

                bufferQueue.registerObserver(qo);
            }
        }

        public IOEvents.DataIOEvent take() throws InterruptedException {
            IOEvents.DataIOEvent event = null;
            while(event == null) {
                final BufferQueue<IOEvents.DataIOEvent> bq = absorberQueue.take();
                event = bq.poll();
                if (event != null)
                    absorberQueue.add(bq);
            }
            return event;
        }
    }
}
