package de.tuberlin.aura.taskmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.statistic.MeasurementManager;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import de.tuberlin.aura.core.task.common.TaskManagerContext;
import de.tuberlin.aura.core.zookeeper.ZookeeperConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZookeeperHelper;

public final class TaskManager implements WM2TMProtocol {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    private final IOManager ioManager;

    private final IORedispatcher ioHandler;

    private final RPCManager rpcManager;

    private final TaskExecutionManager executionManager;

    private final ZooKeeper zookeeper;

    private final TaskManagerContext managerContext;

    private final MachineDescriptor workloadManagerMachine;

    private final MachineDescriptor ownMachine;

    private final Map<UUID, TaskDriverContext> deployedTasks;

    private final Map<UUID, List<TaskDriverContext>> deployedTopologyTasks;

    private final MemoryManager.BufferMemoryManager bufferMemoryManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskManager(final String zookeeperServer, int dataPort, int controlPort) {
        this(zookeeperServer, DescriptorFactory.createMachineDescriptor(dataPort, controlPort));
    }

    public TaskManager(final String zookeeperServer, final MachineDescriptor machine) {
        // sanity check.
        ZookeeperHelper.checkConnectionString(zookeeperServer);

        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        this.ownMachine = machine;

        this.deployedTasks = new HashMap<>();

        this.deployedTopologyTasks = new HashMap<>();

        // Setup buffer memory management.
        this.bufferMemoryManager = new MemoryManager.BufferMemoryManager(machine);

        // Setup execution manager.
        this.executionManager = new TaskExecutionManager(ownMachine, this.bufferMemoryManager);

        this.executionManager.addEventListener(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                unregisterTask((TaskDriverContext) e.getPayload());
            }
        });

        // setup IO.
        this.ioManager = setupIOManager(machine, executionManager);
        this.executionManager.setIOManager(this.ioManager);

        this.rpcManager = new RPCManager(ioManager);

        // setup IORedispatcher.
        this.ioHandler = new IORedispatcher();

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, ioHandler);

        this.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, ioHandler);

        // setup zookeeper.
        this.zookeeper = setupZookeeper(zookeeperServer);

        this.workloadManagerMachine =
                (MachineDescriptor) ZookeeperHelper.readFromZookeeper(this.zookeeper, ZookeeperHelper.ZOOKEEPER_WORKLOADMANAGER);

        // check postcondition.
        if (workloadManagerMachine == null)
            throw new IllegalStateException("workloadManagerMachine == null");

        // setup RPC between workload manager and task manager.
        rpcManager.registerRPCProtocolImpl(this, WM2TMProtocol.class);

        ioManager.connectMessageChannelBlocking(workloadManagerMachine);

        // setup task manager context.
        this.managerContext = new TaskManagerContext(ioManager, rpcManager, executionManager, workloadManagerMachine, ownMachine);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param taskDeploymentDescriptor
     */
    @Override
    public void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor) {
        // sanity check.
        if (taskDeploymentDescriptor == null)
            throw new IllegalArgumentException("taskDescriptor == null");

        final TaskDriverContext taskDriverCtx = registerTask(taskDeploymentDescriptor);
        executionManager.scheduleTask(taskDriverCtx);
    }

    /**
     * @param deploymentDescriptors
     */
    @Override
    public void installTasks(final List<TaskDeploymentDescriptor> deploymentDescriptors) {
        // sanity check.
        if (deploymentDescriptors == null)
            throw new IllegalArgumentException("deploymentDescriptors == null");

        for (final TaskDeploymentDescriptor tdd : deploymentDescriptors) {
            final TaskDriverContext taskDriverCtx = registerTask(tdd);
            executionManager.scheduleTask(taskDriverCtx);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * Get a connection to ZooKeeper and initialize the directories in ZooKeeper.
     * 
     * @return Zookeeper instance.
     */
    private ZooKeeper setupZookeeper(final String zookeeperServer) {
        try {
            final ZooKeeper zookeeper =
                    new ZooKeeper(zookeeperServer, ZookeeperHelper.ZOOKEEPER_TIMEOUT, new ZookeeperConnectionWatcher(new IEventHandler() {

                        @Override
                        public void handleEvent(Event event) {}
                    }));

            ZookeeperHelper.initDirectories(zookeeper);
            final String zkTaskManagerDir = ZookeeperHelper.ZOOKEEPER_TASKMANAGERS + "/" + ownMachine.uid.toString();
            ZookeeperHelper.storeInZookeeper(zookeeper, zkTaskManagerDir, ownMachine);

            return zookeeper;

        } catch (IOException | KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param machineDescriptor
     * @return
     */
    private IOManager setupIOManager(final MachineDescriptor machineDescriptor, final TaskExecutionManager executionManager) {
        final IOManager ioManager = new IOManager(machineDescriptor, executionManager);
        return ioManager;
    }

    /**
     * @param taskDeploymentDescriptor
     * @return
     */
    private TaskDriverContext registerTask(final TaskDeploymentDescriptor taskDeploymentDescriptor) {

        final UUID taskID = taskDeploymentDescriptor.taskDescriptor.taskID;

        if (deployedTasks.containsKey(taskID))
            throw new IllegalStateException("task already deployed");

        // Create an task driver for the submitted task.
        final TaskDriver taskDriver = new TaskDriver(managerContext, taskDeploymentDescriptor);
        final TaskDriverContext taskDriverCtx = taskDriver.getTaskDriverContext();
        deployedTasks.put(taskID, taskDriverCtx);

        final UUID topologyID = taskDeploymentDescriptor.taskDescriptor.topologyID;
        List<TaskDriverContext> contexts = deployedTopologyTasks.get(topologyID);

        if (contexts == null) {
            contexts = new CopyOnWriteArrayList<>();
            deployedTopologyTasks.put(topologyID, contexts);
        }

        contexts.add(taskDriverCtx);
        return taskDriverCtx;
    }

    /**
     * @param taskDriverCtx
     */
    private void unregisterTask(final TaskDriverContext taskDriverCtx) {
        // sanity check.
        if (taskDriverCtx == null)
            throw new IllegalArgumentException("taskDriverCtx == null");

        if (deployedTasks.remove(taskDriverCtx.taskDescriptor.taskID) == null)
            throw new IllegalStateException("task is not deployed");

        final List<TaskDriverContext> taskList = deployedTopologyTasks.get(taskDriverCtx.taskDescriptor.topologyID);

        if (taskList == null)
            throw new IllegalStateException();

        if (!taskList.remove(taskDriverCtx))
            throw new IllegalStateException();

        if (taskList.size() == 0)
            deployedTopologyTasks.remove(taskDriverCtx.taskDescriptor.topologyID);

        LOG.trace("Shutdown event dispatchers");
        taskDriverCtx.driverDispatcher.shutdown();
        taskDriverCtx.taskFSM.shutdown();
    }

    /**
     * @param event
     */
    private void dispatchRemoteTaskTransition(final IOEvents.TaskControlIOEvent event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");
        if (!(event.getPayload() instanceof StateMachine.FSMTransitionEvent))
            throw new IllegalArgumentException("event is not FSMTransitionEvent");

        final List<TaskDriverContext> ctxList = deployedTopologyTasks.get(event.getTopologyID());
        if (ctxList == null) {
            // throw new IllegalArgumentException("ctxList == null");
            LOG.info("Task driver context for topology [" + event.getTopologyID() + "] is removed");
        } else {
            for (final TaskDriverContext ctx : ctxList) {
                if (ctx.taskDescriptor.taskID.equals(event.getTaskID())) {
                    ctx.taskFSM.dispatchEvent((Event) event.getPayload());
                }
            }
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    private final class IORedispatcher extends EventHandler {

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleDataOutputChannelEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).driverDispatcher.dispatchEvent(event);
        }

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleDataInputChannelEvent(final DataIOEvent event) {
            deployedTasks.get(event.dstTaskID).driverDispatcher.dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN)
        private void handleDataChannelGateOpenEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).driverDispatcher.dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE)
        private void handleDataChannelGateCloseEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).driverDispatcher.dispatchEvent(event);
        }

        @Handle(event = IOEvents.TaskControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION)
        private void handleTaskStateTransitionEvent(final IOEvents.TaskControlIOEvent event) {
            dispatchRemoteTaskTransition(event);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        int dataPort = -1;
        int controlPort = -1;
        String zkServer = null;
        String measurementPath = null;
        if (args.length == 4) {
            try {
                zkServer = args[0];
                dataPort = Integer.parseInt(args[1]);
                controlPort = Integer.parseInt(args[2]);
                measurementPath = args[3];
            } catch (NumberFormatException e) {
                LOG.error("Argument" + " must be an integer", e);
                System.exit(1);
            }
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("Args: ");
            for (int i = 0; i < args.length; i++) {
                builder.append(args[i]);
                builder.append("|");
            }

            LOG.error(builder.toString());
            System.exit(1);
        }

        MeasurementManager.setRoot(measurementPath);

        long start = System.nanoTime();
        new TaskManager(zkServer, dataPort, controlPort);
    }
}
