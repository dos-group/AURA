package de.tuberlin.aura.taskmanager;

import java.util.*;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.memory.BufferMemoryManager;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.task.spi.ITaskManager;
import de.tuberlin.aura.storage.DataStorageDriver;

public final class TaskManager implements ITaskManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskManager.class);

    private final IConfig config;

    private final IOManager ioManager;

    private final RPCManager rpcManager;

    private final ITaskExecutionManager executionManager;

    private final IBufferMemoryManager bufferMemoryManager;

    private final IORedispatcher ioHandler;

    private final MachineDescriptor workloadManagerMachine;

    private final MachineDescriptor taskManagerMachine;

    private final Map<UUID, ITaskDriver> deployedTasks;

    private final Map<UUID, List<ITaskDriver>> deployedTopologyTasks;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskManager(IConfig config) {
        final String zkServer = config.getString("zookeeper.server.address");

        // sanity check.
        ZookeeperClient.checkConnectionString(zkServer);

        this.config = config;

        this.taskManagerMachine = DescriptorFactory.createMachineDescriptor(config.getConfig("tm"));

        this.deployedTasks = new HashMap<>();

        this.deployedTopologyTasks = new HashMap<>();

        // Setup buffer memory management.
        this.bufferMemoryManager = new BufferMemoryManager(taskManagerMachine, config.getConfig("tm"));

        // Setup execution manager.
        this.executionManager = new TaskExecutionManager(taskManagerMachine, this.bufferMemoryManager, config.getInt("tm.execution.units.number"));

        this.executionManager.addEventListener(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                unregisterTask((ITaskDriver) e.getPayload());
            }
        });

        this.ioManager = new IOManager(taskManagerMachine, executionManager, config.getConfig("tm.io"));

        this.rpcManager = new RPCManager(ioManager, config.getConfig("tm.io.rpc"));

        this.ioHandler = new IORedispatcher();
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, ioHandler);
        this.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, ioHandler);


        ZookeeperClient zookeeperClient = setupZookeeper(zkServer);

        try {
            this.workloadManagerMachine =
                    (MachineDescriptor) zookeeperClient.read(ZookeeperClient.ZOOKEEPER_WORKLOADMANAGER);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

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
     * @param deploymentDescriptor
     */
    @Override
    public void installTask(final Descriptors.DeploymentDescriptor deploymentDescriptor) {
        // sanity check.
        if (deploymentDescriptor == null)
            throw new IllegalArgumentException("nodeDescriptor == null");

        final ITaskDriver taskDriver = registerTask(deploymentDescriptor);
        executionManager.scheduleTask(taskDriver);
    }

    /**
     * @param deploymentDescriptors
     */
    @Override
    public void installTasks(final List<Descriptors.DeploymentDescriptor> deploymentDescriptors) {
        // sanity check.
        if (deploymentDescriptors == null)
            throw new IllegalArgumentException("deploymentDescriptors == null");

        for (final Descriptors.DeploymentDescriptor tdd : deploymentDescriptors) {
            final ITaskDriver taskDriver = registerTask(tdd);
            executionManager.scheduleTask(taskDriver);
        }
    }

    /**
     * 
     * @param taskID
     * @param outputBinding
     */
    @Override
    public void addOutputBinding(final UUID taskID, final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");
        if (outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");

        final ITaskDriver taskDriver = deployedTasks.get(taskID);

        if (taskDriver == null)
            throw new IllegalStateException("driver == null");

        if (taskDriver.getInvokeable() instanceof DataStorageDriver) {
            final DataStorageDriver ds = (DataStorageDriver) taskDriver.getInvokeable();
            ds.createOutputBinding(outputBinding);
        } else {

            final AbstractInvokeable ai = taskDriver.getDataProducer().getStorage();

            if (ai == null) {
                throw new IllegalStateException("node has no storage");
            } else {
                final DataStorageDriver ds = (DataStorageDriver) ai;
                ds.createOutputBinding(outputBinding);
            }
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
    private ZookeeperClient setupZookeeper(final String zkServer) {
        try {

            ZookeeperClient client = new ZookeeperClient(zkServer);
            client.initDirectories();

            final String zkTaskManagerDir = ZookeeperClient.ZOOKEEPER_TASKMANAGERS + "/" + taskManagerMachine.uid.toString();
            client.store(zkTaskManagerDir, taskManagerMachine);

            return client;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param deploymentDescriptor
     * @return
     */
    private ITaskDriver registerTask(final Descriptors.DeploymentDescriptor deploymentDescriptor) {

        final UUID taskID = deploymentDescriptor.nodeDescriptor.taskID;

        if (deployedTasks.containsKey(taskID))
            throw new IllegalStateException("task already deployed");

        // Create an task driver for the submitted task.
        final ITaskDriver taskDriver = new TaskDriver(this, deploymentDescriptor);
        deployedTasks.put(taskID, taskDriver);

        final UUID topologyID = deploymentDescriptor.nodeDescriptor.topologyID;
        List<ITaskDriver> contexts = deployedTopologyTasks.get(topologyID);

        if (contexts == null) {
            contexts = Collections.synchronizedList(new ArrayList<ITaskDriver>());
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
            throw new IllegalArgumentException("driver == null");

        if (deployedTasks.remove(taskDriver.getNodeDescriptor().taskID) == null)
            throw new IllegalStateException("task is not deployed");

        final List<ITaskDriver> taskList = deployedTopologyTasks.get(taskDriver.getNodeDescriptor().topologyID);

        if (taskList == null)
            throw new IllegalStateException();

        if (!taskList.remove(taskDriver))
            throw new IllegalStateException();

        if (taskList.size() == 0)
            deployedTopologyTasks.remove(taskDriver.getNodeDescriptor().topologyID);

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
                if (taskDriver.getNodeDescriptor().taskID.equals(event.getTaskID())) {
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

        @Handle(event = IOEvents.DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleDataOutputChannelEvent(final DataIOEvent event) {
            deployedTasks.get(event.srcTaskID).dispatchEvent(event);
        }

        @Handle(event = IOEvents.DataIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)
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

    /**
     * TaskManager entry point.
     * 
     * @param args
     */
    public static void main(final String[] args) {

        // construct base argument parser
        ArgumentParser parser = getArgumentParser();


        try {
            // parse the arguments and store them as system properties
            for (Map.Entry<String, Object> e : parser.parseArgs(args).getAttrs().entrySet()) {
                System.setProperty(e.getKey(), e.getValue().toString());
            }
            // load configuration
            IConfig config = IConfigFactory.load(IConfig.Type.TM);

            // start the task manager
            long start = System.nanoTime();
            new TaskManager(config);
            LOG.info("TM startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
        } catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e.getMessage()));
            System.exit(1);
        }
    }

    private static ArgumentParser getArgumentParser() {
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("aura-tm")
                .defaultHelp(true)
                .description("AURA TaskManager.");

        parser.addArgument("--config-dir")
                .type(new FileArgumentType().verifyIsDirectory().verifyCanRead())
                .dest("aura.path.config")
                .setDefault("config")
                .metavar("PATH")
                .help("config folder");
        parser.addArgument("--log-dir")
                .type(new FileArgumentType().verifyIsDirectory().verifyCanRead())
                .dest("aura.path.log")
                .setDefault("log")
                .metavar("PATH")
                .help("log folder");
        parser.addArgument("--zookeeper-url")
                .type(String.class)
                .dest("zookeeper.server.address")
                .metavar("URL")
                .help("zookeeper server URL");
        parser.addArgument("--data-port")
                .type(Integer.class)
                .dest("tm.io.tcp.port")
                .metavar("PORT")
                .help("port for data transfer");
        parser.addArgument("--control-port")
                .type(Integer.class)
                .dest("tm.io.rpc.port")
                .metavar("PORT")
                .help("port for control messages");
        //@formatter:on

        return parser;
    }
}
