package de.tuberlin.aura.taskmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;

public final class TaskManager implements WM2TMProtocol {

    private final static Object mutex = new Object();

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    private final class IORedispatcher extends EventHandler {

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleDataOutputChannelEvent(final DataIOEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.srcTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleDataInputChannelEvent(final DataIOEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.dstTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN)
        private void handleDataChannelGateOpenEvent(final DataIOEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.srcTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE)
        private void handleDataChannelGateCloseEvent(final DataIOEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.srcTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = DataBufferEvent.class)
        private void handleDataEvent(final DataBufferEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.dstTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)
        private void handleDataExhaustedEvent(final DataIOEvent event) {
            final Pair<TaskRuntimeContext, IEventDispatcher> contextAndHandler = taskContextMap.get(event.dstTaskID);
            contextAndHandler.getSecond().dispatchEvent(event);
        }

        @Handle(event = TaskStateTransitionEvent.class)
        private void handleTaskStateTransitionEvent(final TaskStateTransitionEvent event) {
            final List<TaskRuntimeContext> contextList = topologyTaskContextMap.get(event.topologyID);
            if (contextList == null) {
                throw new IllegalArgumentException("contextList == null");
            }
            for (final TaskRuntimeContext tc : contextList) {
                if (tc.task.taskID.equals(event.taskID)) {
                    tc.dispatcher.dispatchEvent(event);
                }
            }
        }
    }

    /**
     *
     */
    private final class TaskEventHandler extends EventHandler {

        public TaskRuntimeContext context;

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleTaskInputDataChannelConnect(final IOEvents.GenericIOEvent event) {
            int gateIndex = 0;
            boolean allInputGatesConnected = true, connectingToCorrectTask = false;
            for (final List<TaskDescriptor> inputGate : context.taskBinding.inputGateBindings) {
                int channelIndex = 0;
                boolean allInputChannelsPerGateConnected = true;
                for (TaskDescriptor inputTask : inputGate) {
                    // get the right input gate for src the event comes from.
                    if (inputTask.taskID.equals(event.srcTaskID)) {
                        // wire queue to input gate
                        final DataReader channelReader = (DataReader) event.payload;
                        // create queue, if there is none yet
                        // as we can have multiple channels insert in one queue (aka multiple
                        // channels per gate)
                        BufferQueue<DataIOEvent> queue = context.queueManager.getInputQueue(gateIndex);
                        channelReader.bindQueue(context.task.taskID, event.getChannel(), gateIndex, channelIndex, queue);

                        context.inputGates.get(gateIndex).setChannelReader(channelReader);

                        LOG.info("INPUT CONNECTION FROM " + inputTask.name + " [" + inputTask.taskID + "] TO TASK " + context.task.name + " ["
                                + context.task.taskID + "] IS ESTABLISHED");
                        connectingToCorrectTask |= true;
                    }
                    boolean connected = false;
                    if (context.inputGates.get(gateIndex).getChannelReader() != null) {
                        connected = context.inputGates.get(gateIndex).getChannelReader().isConnected(context.task.taskID, gateIndex, channelIndex);
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
                context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                              context.task.taskID,
                                                                              TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
            }
        }

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleTaskOutputDataChannelConnect(final IOEvents.GenericIOEvent event) {
            int gateIndex = 0;
            boolean allOutputGatesConnected = true;
            for (final List<TaskDescriptor> outputGate : context.taskBinding.outputGateBindings) {
                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;
                for (TaskDescriptor outputTask : outputGate) {
                    // Set the channel on right position.
                    if (outputTask.taskID.equals(event.dstTaskID)) {
                        // get the right queue manager for task context
                        BufferQueue<DataIOEvent> queue = context.queueManager.getOutputQueue(gateIndex, channelIndex);
                        BufferQueue<DataIOEvent> buffer = context.queueManager.getIntermediateBuffer(gateIndex, channelIndex);

                        DataWriter.ChannelWriter channelWriter = (DataWriter.ChannelWriter) event.payload;
                        channelWriter.setOutputQueue(queue);
                        channelWriter.setBufferQueue(buffer);

                        context.outputGates.get(gateIndex).setChannelWriter(channelIndex, channelWriter);

                        LOG.info("OUTPUT CONNECTION FROM " + context.task.name + " [" + context.task.taskID + "] TO TASK " + outputTask.name + " ["
                                + outputTask.taskID + "] IS ESTABLISHED");
                    }
                    // all data outputs are connected...
                    allOutputChannelsPerGateConnected &= (context.outputGates.get(gateIndex).getChannelWriter(channelIndex++) != null);
                }
                allOutputGatesConnected &= allOutputChannelsPerGateConnected;
                ++gateIndex;
            }

            if (allOutputGatesConnected) {
                context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                              context.task.taskID,
                                                                              TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED));
            }
        }

        // @Handle(event = DataBufferEvent.class)
        // private void handleTaskInputData(final DataBufferEvent event) {
        // context.inputGates.get(context.getInputGateIndexFromTaskID(event.srcTaskID))
        // .addToInputQueue(event);
        // }
        //
        // @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)
        // private void handleTaskInputDataExhausted(final DataIOEvent event) {
        // final InputGate ig =
        // context.inputGates.get(context.getInputGateIndexFromTaskID(event.srcTaskID));
        // ig.addToInputQueue(event);
        // }

        private long timeOfLastStateChange = System.currentTimeMillis();

        @Handle(event = TaskStateTransitionEvent.class)
        private void handleTaskStateTransition(final TaskStateTransitionEvent event) {
            synchronized (context.getCurrentTaskState()) { // serialize task state transitions!

                final TaskState oldState = context.getCurrentTaskState();
                final TaskState nextState = context.doTaskStateTransition(event.transition);

                final long currentTime = System.currentTimeMillis();

                final IOEvents.MonitoringEvent.TaskStateUpdate taskStateUpdate =
                        new IOEvents.MonitoringEvent.TaskStateUpdate(context.task.taskID,
                                                                     context.task.name,
                                                                     oldState,
                                                                     nextState,
                                                                     event.transition,
                                                                     currentTime - timeOfLastStateChange);

                timeOfLastStateChange = currentTime;

                final IOEvents.MonitoringEvent monitoringEvent = new IOEvents.MonitoringEvent(context.task.topologyID, taskStateUpdate);
                ioManager.sendEvent(wmMachine.uid, monitoringEvent);

                // Trigger state dependent actions. Realization of a classic Moore automata.
                switch (nextState) {

                    case TASK_STATE_NOT_CONNECTED: {}
                        break;
                    case TASK_STATE_INPUTS_CONNECTED: {}
                        break;
                    case TASK_STATE_OUTPUTS_CONNECTED: {}
                        break;
                    case TASK_STATE_READY: {}
                        break;

                    case TASK_STATE_RUNNING: {

                        switch (oldState) {

                            case TASK_STATE_READY: {
                                scheduleTask(context);
                            }
                                break;

                            case TASK_STATE_PAUSED: {
                                context.getInvokeable().resume();
                            }
                            default:
                                throw new IllegalStateException();
                        }
                    }
                        break;
                    case TASK_STATE_PAUSED: {
                        context.getInvokeable().suspend();
                    }
                        break;
                    case TASK_STATE_FINISHED: {
                        taskContextMap.remove(context.task.taskID);
                        topologyTaskContextMap.get(context.task.topologyID).remove(context);
                        context.close();
                    }
                        break;
                    case TASK_STATE_FAILURE: {
                        taskContextMap.remove(context.task.taskID);
                        topologyTaskContextMap.get(context.task.topologyID).remove(context);
                        context.close();
                    }
                        break;
                    case TASK_STATE_CANCELED: {
                        context.getInvokeable().cancel();
                        taskContextMap.remove(context.task.taskID);
                        topologyTaskContextMap.get(context.task.topologyID).remove(context);
                        context.close();
                    }
                        break;

                    case TASK_STATE_RECOVER: {}
                        break;

                    case TASK_STATE_UNDEFINED: {
                        throw new IllegalStateException("task " + context.task.name + " [" + context.task.taskID + "] from state " + oldState
                                + " to " + context.getCurrentTaskState() + " is not defined  [" + event.transition.toString() + "]");
                    }
                    default:
                        break;
                }
                LOG.info("CHANGE STATE OF TASK " + context.task.name + " [" + context.task.taskID + "] FROM " + oldState + " TO "
                        + context.getCurrentTaskState() + "  [" + event.transition.toString() + "]");
            }
        }
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskManager(final String zkServer, int dataPort, int controlPort) {
        this(zkServer, DescriptorFactory.createMachineDescriptor(dataPort, controlPort));
    }

    public TaskManager(final String zkServer, final MachineDescriptor machine) {
        // sanity check.
        ZkHelper.checkConnectionString(zkServer);

        this.taskContextMap = new ConcurrentHashMap<UUID, Pair<TaskRuntimeContext, IEventDispatcher>>();
        this.topologyTaskContextMap = new ConcurrentHashMap<UUID, List<TaskRuntimeContext>>();
        this.ioManager = new IOManager(machine);
        this.rpcManager = new RPCManager(ioManager);
        this.ioHandler = new IORedispatcher();
        this.codeImplanter = new UserCodeImplanter(this.getClass().getClassLoader());

        final int N = 4;
        this.executionUnit = new TaskExecutionUnit[N];
        for (int i = 0; i < N; ++i) {
            this.executionUnit[i] = new TaskExecutionUnit(i);
            this.executionUnit[i].start();
        }

        final String[] IOEvents =
                {DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
                        DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, DataEventType.DATA_EVENT_BUFFER,
                        DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT};

        this.ioManager.addEventListener(IOEvents, ioHandler);

        // TODO: move this into a seperate mehtod.
        // Get a connection to ZooKeeper and initialize the directories in ZooKeeper.
        try {
            this.zookeeper = new ZooKeeper(zkServer, ZkHelper.ZOOKEEPER_TIMEOUT, new ZkConnectionWatcher(new IEventHandler() {

                @Override
                public void handleEvent(Event event) {}
            }));

            ZkHelper.initDirectories(this.zookeeper);
            ZkHelper.storeInZookeeper(zookeeper, ZkHelper.ZOOKEEPER_TASKMANAGERS + "/" + machine.uid.toString(), machine);
            this.wmMachine = (MachineDescriptor) ZkHelper.readFromZookeeper(zookeeper, ZkHelper.ZOOKEEPER_WORKLOADMANAGER);

            // check postcondition.
            if (wmMachine == null) {
                throw new IllegalStateException("wmMachine == null");
            }

        } catch (IOException | KeeperException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }

        rpcManager.registerRPCProtocolImpl(this, WM2TMProtocol.class);
        ioManager.connectMessageChannelBlocking(wmMachine);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskManager.class);

    private MachineDescriptor wmMachine;

    private final Map<UUID, Pair<TaskRuntimeContext, IEventDispatcher>> taskContextMap;

    private final Map<UUID, List<TaskRuntimeContext>> topologyTaskContextMap;

    private final IOManager ioManager;

    private final IORedispatcher ioHandler;

    private final RPCManager rpcManager;

    private final TaskExecutionUnit[] executionUnit;

    private final UserCodeImplanter codeImplanter;

    private ZooKeeper zookeeper;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor) {
        // sanity check.
        if (taskDeploymentDescriptor == null) {
            throw new IllegalArgumentException("taskDescriptor == null");
        }

        @SuppressWarnings("unchecked")
        final Class<? extends TaskInvokeable> userCodeClass =
                (Class<? extends TaskInvokeable>) codeImplanter.implantUserCodeClass(taskDeploymentDescriptor.taskDescriptor.userCode);

        installTask(taskDeploymentDescriptor.taskDescriptor, taskDeploymentDescriptor.taskBindingDescriptor, userCodeClass);
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private synchronized void wireOutputDataChannels(final TaskDescriptor taskDescriptor, final TaskBindingDescriptor taskBindingDescriptor) {

        // Connect outputs, if we have some...
        if (taskBindingDescriptor.outputGateBindings.size() > 0) {
            for (final List<TaskDescriptor> outputGate : taskBindingDescriptor.outputGateBindings) {
                for (final TaskDescriptor outputTask : outputGate) {
                    ioManager.connectDataChannel(taskDescriptor.taskID, outputTask.taskID, outputTask.getMachineDescriptor());
                }
            }
        }
    }

    private void scheduleTask(final TaskRuntimeContext context) {
        // sanity check.
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        final int N = 4;
        int tmpMin, tmpMinOld;
        tmpMin = tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
        int selectedEU = 0;
        for (int i = 1; i < N; ++i) {
            tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
            if (tmpMin < tmpMinOld) {
                tmpMinOld = tmpMin;
                selectedEU = i;
            }
        }
        executionUnit[selectedEU].enqueueTask(context);
        LOG.info("EXECUTE TASK " + context.task.name + " [" + context.task.taskID + "]" + " ON EXECUTIONUNIT ("
                + executionUnit[selectedEU].getExecutionUnitID() + ")");
    }

    private void installTask(final TaskDescriptor taskDescriptor,
                             final TaskBindingDescriptor taskBindingDescriptor,
                             final Class<? extends TaskInvokeable> executableClass) {

        final TaskEventHandler handler = new TaskEventHandler();
        final TaskRuntimeContext context = new TaskRuntimeContext(taskDescriptor, taskBindingDescriptor, handler, executableClass);
        handler.context = context;
        taskContextMap.put(taskDescriptor.taskID, new Pair<TaskRuntimeContext, IEventDispatcher>(context, context.dispatcher));
        LOG.info("CREATE CONTEXT FOR TASK [" + taskDescriptor.taskID + "] ON MACHINE [" + ioManager.machine.uid + "]");

        if (taskBindingDescriptor.inputGateBindings.size() == 0) {
            context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                          context.task.taskID,
                                                                          TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
        }

        if (taskBindingDescriptor.outputGateBindings.size() == 0) {
            context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                          context.task.taskID,
                                                                          TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED));
        }

        // TODO: To allow cycles in the execution graph we have to split up
        // installation and wiring of tasks in the deployment phase!
        wireOutputDataChannels(taskDescriptor, taskBindingDescriptor);

        List<TaskRuntimeContext> contextList = topologyTaskContextMap.get(taskDescriptor.topologyID);
        if (contextList == null) {
            contextList = new ArrayList<TaskRuntimeContext>();
            topologyTaskContextMap.put(taskDescriptor.topologyID, contextList);
        }
        contextList.add(context);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final Logger rootLOG = Logger.getRootLogger();

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        rootLOG.addAppender(consoleAppender);

        int dataPort = -1;
        int controlPort = -1;
        String zkServer = null;
        if (args.length == 3) {
            try {
                zkServer = args[0];
                dataPort = Integer.parseInt(args[1]);
                controlPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Argument" + " must be an integer");
                System.exit(1);
            }
        } else {
            System.err.println("only two numeric arguments allowed: dataPort, controlPort");
            System.exit(1);
        }

        new TaskManager(zkServer, dataPort, controlPort);
    }

    public RPCManager getRPCManager() {
        return rpcManager;
    }

    public IOManager getIOManager() {
        return ioManager;
    }
}
