package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
import de.tuberlin.aura.core.task.common.TaskManagerContext;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;

public final class TaskManager implements WM2TMProtocol {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskManager.class);

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

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskManager(final String zookeeperServer, int dataPort, int controlPort) {
        this(zookeeperServer, DescriptorFactory.createMachineDescriptor(dataPort, controlPort));
    }

    public TaskManager(final String zookeeperServer, final MachineDescriptor machine) {
        // sanity check.
        ZkHelper.checkConnectionString(zookeeperServer);

        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        this.ownMachine = machine;

        this.deployedTasks = new HashMap<>();

        this.deployedTopologyTasks = new HashMap<>();

        // setup IO and execution managers.
        this.ioManager = new IOManager(machine);

        this.rpcManager = new RPCManager(ioManager);

        this.executionManager = new TaskExecutionManager(ownMachine);

        this.executionManager.addEventListener(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK,
                new IEventHandler() {

                    @Override
                    public void handleEvent(Event e) {
                        unregisterTask((TaskDriverContext) e.getPayload());
                    }
                });

        // setup IORedispatcher.
        this.ioHandler = new IORedispatcher();

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, ioHandler);

        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, ioHandler);

        this.ioManager.addEventListener(TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT, ioHandler);

        // setup zookeeper.
        this.zookeeper = setupZookeeper(zookeeperServer);

        this.workloadManagerMachine = (MachineDescriptor) ZkHelper.readFromZookeeper(this.zookeeper, ZkHelper.ZOOKEEPER_WORKLOADMANAGER);

        // check postcondition.
        if (workloadManagerMachine == null)
            throw new IllegalStateException("workloadManagerMachine == null");

        // setup RPC between workload manager and task manager.
        rpcManager.registerRPCProtocolImpl(this, WM2TMProtocol.class);

        ioManager.connectMessageChannelBlocking(workloadManagerMachine);

        // setup task manager context.
        this.managerContext =
                new TaskManagerContext(
                        ioManager,
                        rpcManager,
                        executionManager,
                        workloadManagerMachine,
                        ownMachine
                );
    }

    // ---------------------------------------------------
    // Public.
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

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    /**
     * Get a connection to ZooKeeper and initialize the directories in ZooKeeper.
     *
     * @return Zookeeper instance.
     */
    private ZooKeeper setupZookeeper(final String zookeeperServer) {
        try {
            final ZooKeeper zookeeper = new ZooKeeper(zookeeperServer, ZkHelper.ZOOKEEPER_TIMEOUT,
                    new ZkConnectionWatcher(
                            new IEventHandler() {

                                @Override
                                public void handleEvent(Event event) {
                                }
                            }
                    )
            );

            ZkHelper.initDirectories(zookeeper);
            final String zkTaskManagerDir = ZkHelper.ZOOKEEPER_TASKMANAGERS + "/" + ownMachine.uid.toString();
            ZkHelper.storeInZookeeper(zookeeper, zkTaskManagerDir, ownMachine);

            return zookeeper;

        } catch (IOException | KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
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
            contexts = new ArrayList<TaskDriverContext>();
            deployedTopologyTasks.put(topologyID, contexts);
        }

        contexts.add(taskDriverCtx);
        return taskDriverCtx;
    }

    /**
     * @param taskDriverCtx
     */
    private void unregisterTask(final TaskDriverContext taskDriverCtx) {

        /*if(deployedTasks.remove(taskDriverCtx.taskDescriptor.taskID) == null)
            throw new IllegalStateException("task is not deployed");

       final List<TaskDriverContext> taskList = deployedTopologyTasks.get(taskDriverCtx.taskDescriptor.topologyID);

        if(taskList == null)
            throw new IllegalStateException();

        if(!taskList.remove(taskDriverCtx))
            throw new IllegalStateException();

        if(taskList.size() == 0)
            deployedTopologyTasks.remove(taskDriverCtx.taskDescriptor.topologyID);*/
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

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

        @Handle(event = TaskStateTransitionEvent.class)
        private void handleTaskStateTransitionEvent(final TaskStateTransitionEvent event) {
            final List<TaskDriverContext> ctxList = deployedTopologyTasks.get(event.topologyID);
            if (ctxList == null) {
                throw new IllegalArgumentException("ctxList == null");
            }
            for (final TaskDriverContext ctx : ctxList) {
                if (ctx.taskDescriptor.taskID.equals(event.taskID)) {
                    ctx.driverDispatcher.dispatchEvent(event);
                }
            }
        }
    }
}
