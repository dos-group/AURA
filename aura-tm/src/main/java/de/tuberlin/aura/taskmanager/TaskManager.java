package de.tuberlin.aura.taskmanager;

import java.io.IOException;
import java.util.*;

import de.tuberlin.aura.core.memory.BufferMemoryManager;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

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
import de.tuberlin.aura.core.measurement.MeasurementManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.task.spi.ITaskManager;
import de.tuberlin.aura.core.zookeeper.ZookeeperConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZookeeperHelper;

public final class TaskManager implements ITaskManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskManager.class);


    private final IOManager ioManager;

    private final RPCManager rpcManager;

    private final ITaskExecutionManager executionManager;

    private final IBufferMemoryManager bufferMemoryManager;

    private final ZooKeeper zookeeper;


    private final IORedispatcher ioHandler;

    private final MachineDescriptor workloadManagerMachine;

    private final MachineDescriptor taskManagerMachine;

    private final Map<UUID, ITaskDriver> deployedTasks;

    private final Map<UUID, List<ITaskDriver>> deployedTopologyTasks;

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

        this.taskManagerMachine = machine;

        this.deployedTasks = new HashMap<>();

        this.deployedTopologyTasks = new HashMap<>();

        // Setup buffer memory management.
        this.bufferMemoryManager = new BufferMemoryManager(machine);

        // Setup execution manager.
        this.executionManager = new TaskExecutionManager(taskManagerMachine, this.bufferMemoryManager);

        this.executionManager.addEventListener(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                unregisterTask((ITaskDriver) e.getPayload());
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
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public IOManager getIOManager() {
        return ioManager;
    }

    @Override
    public RPCManager getRPCManager() {
        return rpcManager;
    }

    @Override
    public ITaskExecutionManager getTaskExecutionManager() {
        return executionManager;
    }

    @Override
    public MachineDescriptor getWorkloadManagerMachineDescriptor() {
        return workloadManagerMachine;
    }

    @Override
    public MachineDescriptor getTaskManagerMachineDescriptor() {
        return taskManagerMachine;
    }

    // ------------- Workload Manager Protocol ---------------

    /**
     * @param taskDeploymentDescriptor
     */
    @Override
    public void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor) {
        // sanity check.
        if (taskDeploymentDescriptor == null)
            throw new IllegalArgumentException("taskDescriptor == null");

        final ITaskDriver taskDriver = registerTask(taskDeploymentDescriptor);
        executionManager.scheduleTask(taskDriver);
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
            final ITaskDriver taskDriver = registerTask(tdd);
            executionManager.scheduleTask(taskDriver);
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
            final String zkTaskManagerDir = ZookeeperHelper.ZOOKEEPER_TASKMANAGERS + "/" + taskManagerMachine.uid.toString();
            ZookeeperHelper.storeInZookeeper(zookeeper, zkTaskManagerDir, taskManagerMachine);

            return zookeeper;

        } catch (IOException | KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param machineDescriptor
     * @return
     */
    private IOManager setupIOManager(final MachineDescriptor machineDescriptor, final ITaskExecutionManager executionManager) {
        final IOManager ioManager = new IOManager(machineDescriptor, executionManager);
        return ioManager;
    }

    /**
     * @param taskDeploymentDescriptor
     * @return
     */
    private ITaskDriver registerTask(final TaskDeploymentDescriptor taskDeploymentDescriptor) {

        final UUID taskID = taskDeploymentDescriptor.taskDescriptor.taskID;

        if (deployedTasks.containsKey(taskID))
            throw new IllegalStateException("task already deployed");

        // Create an task driver for the submitted task.
        final ITaskDriver taskDriver = new TaskDriver(this, taskDeploymentDescriptor);
        deployedTasks.put(taskID, taskDriver);

        final UUID topologyID = taskDeploymentDescriptor.taskDescriptor.topologyID;
        List<ITaskDriver> contexts = deployedTopologyTasks.get(topologyID);

        if (contexts == null) {
            contexts = new ArrayList<>();
            deployedTopologyTasks.put(topologyID, contexts);
        }

        contexts.add(taskDriver);
        return taskDriver;
    }

    /**
     * @param taskDriver
     */
    private void unregisterTask(final ITaskDriver taskDriver) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("taskDriver == null");

        if (deployedTasks.remove(taskDriver.getTaskDescriptor().taskID) == null)
            throw new IllegalStateException("task is not deployed");

        final List<ITaskDriver> taskList = deployedTopologyTasks.get(taskDriver.getTaskDescriptor().topologyID);

        if (taskList == null)
            throw new IllegalStateException();

        if (!taskList.remove(taskDriver))
            throw new IllegalStateException();

        if (taskList.size() == 0)
            deployedTopologyTasks.remove(taskDriver.getTaskDescriptor().topologyID);

        LOG.trace("Shutdown event dispatchers");
        taskDriver.shutdown();
        taskDriver.getTaskStateMachine().shutdown();
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

        final List<ITaskDriver> taskList = deployedTopologyTasks.get(event.getTopologyID());
        if (taskList == null) {
            // throw new IllegalArgumentException("ctxList == null");
            LOG.info("Task driver context for topology [" + event.getTopologyID() + "] is removed");
        } else {
            for (final ITaskDriver taskDriver : taskList) {
                if (taskDriver.getTaskDescriptor().taskID.equals(event.getTaskID())) {
                    taskDriver.getTaskStateMachine().dispatchEvent((Event) event.getPayload());
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
            deployedTasks.get(event.srcTaskID).dispatchEvent(event);
        }

        @Handle(event = IOEvents.GenericIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleDataInputChannelEvent(final DataIOEvent event) {
            deployedTasks.get(event.dstTaskID).dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN)
        private void handleDataChannelGateOpenEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).dispatchEvent(event);
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE)
        private void handleDataChannelGateCloseEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).dispatchEvent(event);
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
