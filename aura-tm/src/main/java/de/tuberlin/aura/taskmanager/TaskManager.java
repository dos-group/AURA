package de.tuberlin.aura.taskmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import de.tuberlin.aura.core.record.Partitioner;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.memory.BufferMemoryManager;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;
import de.tuberlin.aura.core.taskmanager.spi.ITaskDriver;
import de.tuberlin.aura.core.taskmanager.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.taskmanager.spi.ITaskManager;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.datasets.DatasetDriver;

public final class TaskManager implements ITaskManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskManager.class);

    public final IIOManager ioManager;

    public final IRPCManager rpcManager;

    public final ITaskExecutionManager executionManager;

    public final IBufferMemoryManager bufferMemoryManager;

    private final MachineDescriptor workloadManagerMachine;

    private final MachineDescriptor taskManagerMachine;

    private final Map<UUID, ITaskDriver> deployedTasks;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskManager(final IConfig config) {
        // Sanity check.
        if (config == null)
            throw new IllegalArgumentException("config == null");

        this.deployedTasks = new ConcurrentHashMap<>();

        // Generate MachineDescriptor for this TaskManager.
        this.taskManagerMachine = DescriptorFactory.createMachineDescriptor(config.getConfig("tm"));
        // Initialize BufferManager.
        this.bufferMemoryManager = new BufferMemoryManager(taskManagerMachine, config.getConfig("tm"));
        // Initialize ExecutionManager.
        this.executionManager = new TaskExecutionManager(this, taskManagerMachine, this.bufferMemoryManager, config.getInt("tm.execution.units.number"));
        // Initialize IOManager.
        this.ioManager = new IOManager(taskManagerMachine, executionManager, config.getConfig("tm.io"));
        // Initialize RPC Manager.
        this.rpcManager = new RPCManager(ioManager, config.getConfig("tm.io.rpc"));

        final IORedispatcher ioHandler = new IORedispatcher();
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, ioHandler);
        this.ioManager.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, ioHandler);
        this.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, ioHandler);

        // Initialize Zookeeper.
        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zkServer);
        ZookeeperClient zookeeperClient = initializeZookeeper(zkServer);

        try {
            this.workloadManagerMachine =
                    (MachineDescriptor) zookeeperClient.read(ZookeeperClient.ZOOKEEPER_WORKLOADMANAGER);
            if (workloadManagerMachine == null)
                throw new IllegalStateException("workloadManagerMachine == null");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        // Configure RPC between WorkloadManager and TaskManager.
        rpcManager.registerRPCProtocol(this, IWM2TMProtocol.class);
        ioManager.connectMessageChannelBlocking(workloadManagerMachine);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void installTask(final Descriptors.DeploymentDescriptor deploymentDescriptor) {
        // sanity check.
        if (deploymentDescriptor == null)
            throw new IllegalArgumentException("nodeDescriptor == null");
        final ITaskDriver taskDriver = registerTask(deploymentDescriptor);
        executionManager.scheduleTask(taskDriver);
    }

    @Override
    public void addOutputBinding(final UUID taskID,
                                 final UUID topologyID,
                                 final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding,
                                 final Partitioner.PartitioningStrategy partitioningStrategy,
                                 final int[][] partitioningKeys) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if (outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");
        if (partitioningStrategy == null)
            throw new IllegalArgumentException("partitioningStrategy == null");
        if (partitioningKeys == null)
            throw new IllegalArgumentException("partitioningKeys == null");

        final ITaskDriver taskDriver = deployedTasks.get(taskID);
        if (taskDriver == null)
            throw new IllegalStateException("driver == null");

        if (taskDriver.getInvokeable() instanceof DatasetDriver) {
            final DatasetDriver ds = (DatasetDriver) taskDriver.getInvokeable();
            ds.createOutputBinding(topologyID, outputBinding, partitioningStrategy, partitioningKeys);
        }
    }

    @Override
    public void uninstallTask(final UUID taskID) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");
        final ITaskDriver driver = deployedTasks.get(taskID);
        if (driver == null)
            throw new IllegalStateException("TaskDriver is not found");
        driver.shutdown();
        driver.getTaskStateMachine().shutdown();
        //deployedTasks.remove(taskID);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private ZookeeperClient initializeZookeeper(final String zookeeperServer) {
        // Sanity check.
        if (zookeeperServer == null)
            throw new IllegalArgumentException("zookeeperServer == null");

        try {
            ZookeeperClient client = new ZookeeperClient(zookeeperServer);
            client.initDirectories();
            final String zkTaskManagerDir = ZookeeperClient.ZOOKEEPER_TASKMANAGERS + "/" + taskManagerMachine.uid.toString();
            client.store(zkTaskManagerDir, taskManagerMachine);
            return client;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ITaskDriver registerTask(final Descriptors.DeploymentDescriptor deploymentDescriptor) {
        // Sanity check.
        if (deploymentDescriptor == null)
            throw new IllegalArgumentException("deploymentDescriptor == null");
        final UUID taskID = deploymentDescriptor.nodeDescriptor.taskID;
        if (deployedTasks.containsKey(taskID))
            throw new IllegalStateException("Task is already deployed.");
        // Create an TaskDriver for the submitted task.
        final ITaskDriver taskDriver = new TaskDriver(this, deploymentDescriptor);
        deployedTasks.put(taskID, taskDriver);
        return taskDriver;
    }

    private void dispatchRemoteTaskTransition(final IOEvents.TaskControlIOEvent event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");
        if (!(event.getPayload() instanceof StateMachine.FSMTransitionEvent))
            throw new IllegalArgumentException("event is not FSMTransitionEvent");
        for (final ITaskDriver taskDriver : deployedTasks.values()) {
            if (taskDriver.getNodeDescriptor().taskID.equals(event.getTaskID())) {
                taskDriver.getTaskStateMachine().dispatchEvent((Event) event.getPayload());
            }
        }
        if (deployedTasks.isEmpty()) {
            LOG.info("Task driver context for topology [" + event.getTopologyID() + "] is removed");
        }
    }

    // ---------------------------------------------------
    // Public Getters.
    // ---------------------------------------------------

    @Override
    public IIOManager getIOManager() {
        return ioManager;
    }

    @Override
    public IRPCManager getRPCManager() {
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

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        // construct base argument parser
        ArgumentParser parser = getArgumentParser();

        try {
            // parse the arguments and store them as system properties
            for (Map.Entry<String, Object> e : parser.parseArgs(args).getAttrs().entrySet()) {
                if (e.getValue() != null)
                    System.setProperty(e.getKey(), e.getValue().toString());
            }

            // start the taskmanager manager
            long start = System.nanoTime();
            new TaskManager(IConfigFactory.load(IConfig.Type.TM));
            LOG.info("TM startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
        } catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e));
            e.printStackTrace();
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
        //@formatter:on

        return parser;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /*private ITaskDriver getDeployedTask(final DataIOEvent event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");
        int i = 0;
        while (!deployedTasks.containsKey(event.srcTaskID)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            if (i == 200) {
                System.err.println("uid to look up: " + event.srcTaskID);

                if (deployedTasks.keySet().isEmpty()) {
                    System.err.println("no deployed tasks available");
                }

                for(final UUID uid : deployedTasks.keySet()) {
                    System.err.println("deployed task: " + uid);
                }
                throw new IllegalStateException("event.srcTaskID");
            }
            i++;
        }
        LOG.info("LÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖFT");
        if (deployedTasks.get(event.srcTaskID) == null) {
            LOG.info("NNNNEEEETTTTTTT -----------------------------");
            throw new IllegalArgumentException("EVENT: src:" + event.srcTaskID + "   dst:" + event.dstTaskID + "   payload:" + event.getPayload());
        }
        return deployedTasks.get(event.srcTaskID);
    }*/

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
}