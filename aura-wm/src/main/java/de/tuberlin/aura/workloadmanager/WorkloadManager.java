package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import de.tuberlin.aura.core.protocols.ITM2WMProtocol;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;
import de.tuberlin.aura.core.taskmanager.TaskManagerStatus;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.spi.IDistributedEnvironment;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.IClientWMProtocol;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.taskmanager.TaskManager;


public class WorkloadManager implements IWorkloadManager, IClientWMProtocol, ITM2WMProtocol {

    // ---------------------------------------------------
    // Execution Modes.
    // ---------------------------------------------------

    public static enum ExecutionMode {

        LOCAL("LOCAL"),

        DISTRIBUTED("DISTRIBUTED");

        final String name;

        private ExecutionMode(String name) {
            this.name = name;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(WorkloadManager.class);

    private final IConfig config;

    private final IIOManager ioManager;

    private final IRPCManager rpcManager;

    private final IInfrastructureManager infrastructureManager;

    private final IDistributedEnvironment environmentManager;

    private final MachineDescriptor machineDescriptor;

    private final Map<UUID, TopologyController> registeredTopologies;

    private final Map<UUID, Set<UUID>> registeredSessions;

    private final ExecutorService executor;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public WorkloadManager(final IConfig config) {
        // Sanity check.
        if (config == null)
            throw new IllegalArgumentException("config == null");

        this.config = config;

        this.registeredTopologies = new ConcurrentHashMap<>();

        this.registeredSessions = new ConcurrentHashMap<>();

        this.executor = Executors.newFixedThreadPool(2);

        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zkServer);

        // Generate MachineDescriptor for WorkloadManager.
        this.machineDescriptor = DescriptorFactory.createMachineDescriptor(config.getConfig("wm"));
        // Initialize IOManager.
        this.ioManager = new IOManager(this.machineDescriptor, null, config.getConfig("wm.io"));

        // Register EventHandler (acts as an Dispatcher) for TaskManager internalState updates.
        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                try {
                    registeredTopologies.get(event.getTopologyID()).dispatchEvent(event);
                } catch(Exception ex) {
                    if (event == null)
                        System.out.println("(1)");
                    else
                        if (registeredTopologies.get(event.getTopologyID()) == null)
                            System.out.println("(2)");

                    throw new IllegalStateException(ex);
                }
            }
        });
        // Register EventHandler (acts as an Dispatcher) for TaskManager transitions.
        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).getTopologyFSM().dispatchEvent((Event) event.getPayload());
            }
        });
        // Register EventHandler for Client iteration evaluation.
        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_CLIENT_ITERATION_EVALUATION, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.ClientControlIOEvent event = (IOEvents.ClientControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).dispatchEvent(e);
            }
        });

        // Initialize RPC Manager.
        this.rpcManager = new RPCManager(ioManager, config.getConfig("wm.io.rpc"));
        // Register RPC Protocols.
        rpcManager.registerRPCProtocol(this, IClientWMProtocol.class);
        rpcManager.registerRPCProtocol(this, ITM2WMProtocol.class);

        // Initialize InfrastructureManager.
        this.infrastructureManager = new InfrastructureManager(this, zkServer, machineDescriptor, config);
        // Initialize InfrastructureManager.
        this.environmentManager = new DistributedEnvironment();
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public synchronized void openSession(final UUID sessionID) {
        // Sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");
        if (registeredSessions.containsKey(sessionID))
            throw new IllegalStateException("session with this ID [" + sessionID.toString() + "] already exists");

        // register a new session for a examples.
        registeredSessions.put(sessionID, new HashSet<UUID>());
        LOG.info("OPENED SESSION [" + sessionID + "]");
    }

    @Override
    public synchronized void submitTopology(final UUID sessionID, final AuraTopology topology) {
        // Sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        final Set<UUID> topologiesAssignedToSession = registeredSessions.get(sessionID);

        if (topologiesAssignedToSession != null) {

            if (topologiesAssignedToSession.contains(topology.topologyID) || registeredTopologies.containsKey(topology.topologyID))
                throw new IllegalStateException("topology " + topology.topologyID + "  already running");

            topologiesAssignedToSession.add(topology.topologyID);

            final TopologyController topologyController = new TopologyController(this, topology.topologyID, topology, this.config);
            registeredTopologies.put(topology.topologyID, topologyController);

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    topologyController.assembleTopology();
                }
            });

        } else {
            throw new IllegalStateException("unknown session id " + sessionID);
        }
        LOG.info("TOPOLOGY '" + topology.name + "' SUBMITTED");
    }

    @Override
    public synchronized void closeSession(final UUID sessionID) {
        // Sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");

        final Set<UUID> assignedTopologies = registeredSessions.remove(sessionID);

        if (assignedTopologies == null)
            throw new IllegalStateException("session with id " + sessionID.toString() + " does not exist");

        LOG.info("CLOSED SESSION [" + sessionID + "]");
    }

    public void unregisterTopology(final UUID topologyID) {
        // Sanity check.
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");

        final TopologyController finishedTopologyController = registeredTopologies.remove(topologyID);

        if (finishedTopologyController == null)
            throw new IllegalStateException("topologyID not found");

        if (!finishedTopologyController.getTopologyFSM().isInFinalState()) {
            throw new IllegalArgumentException("topology " + topologyID + " is not in final internalState");
            //finishedTopologyController.cancelTopology(); // TODO: Not implemented yet!
        }

        executor.submit(new Runnable() {
            @Override
            public void run() {
                finishedTopologyController.shutdownTopologyController();
            }
        });

        // TODO: maybe critical?

        AuraTopology finishedTopology = finishedTopologyController.getTopology();
        this.infrastructureManager.reclaimExecutionUnits(finishedTopology);
        for (final Set<UUID> topologiesAssignedToSession : registeredSessions.values()) {
            if (topologiesAssignedToSession.contains(topologyID))
                topologiesAssignedToSession.remove(topologyID);
        }
    }

    @Override
    public <E> Collection<E> gatherDataset(final UUID uid) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");

        final Topology.DatasetNode dataset = environmentManager.getDataset(uid);
        final List<E> data = new ArrayList<>();
        for (final Topology.ExecutionNode en : dataset.getExecutionNodes()) {
            final IWM2TMProtocol tmProtocol =
                    rpcManager.getRPCProtocolProxy(IWM2TMProtocol.class, en.getNodeDescriptor().getMachineDescriptor());
            final Collection<E> datasetPartition = (Collection<E>)tmProtocol.getDataset(en.getNodeDescriptor().taskID);
            data.addAll(datasetPartition);
        }

        return data;
    }

    @Override
    public <E> void scatterDataset(final UUID datasetID, final Collection<E> dataset) {
        // sanity check.
        if (datasetID == null)
            throw new IllegalArgumentException("datasetID == null");
        if (dataset == null)
            throw new IllegalArgumentException("dataset == null");

        environmentManager.addBroadcastDataset(datasetID, dataset);
    }

    @Override
    public void eraseDataset(final UUID datasetID) {
        // sanity check.
        if (datasetID == null)
            throw new IllegalArgumentException("datasetID == null");

        final Topology.DatasetNode dataset = environmentManager.getDataset(datasetID);
        for (final Topology.ExecutionNode en : dataset.getExecutionNodes()) {
            final IWM2TMProtocol tmProtocol =
                    rpcManager.getRPCProtocolProxy(IWM2TMProtocol.class, en.getNodeDescriptor().getMachineDescriptor());
            tmProtocol.eraseDataset(en.getNodeDescriptor().taskID);
        }

        infrastructureManager.reclaimExecutionUnits(dataset);
    }

    @Override
    public void assignDataset(final UUID dstDatasetID, final UUID srcDatasetID) {
        // sanity check.
        if (dstDatasetID == null)
            throw new IllegalArgumentException("dstDatasetID == null");
        if (srcDatasetID == null)
            throw new IllegalArgumentException("srcDatasetID == null");

        final Topology.DatasetNode dstDataset = environmentManager.getDataset(dstDatasetID);
        final Topology.DatasetNode srcDataset = environmentManager.getDataset(srcDatasetID);

        if (srcDataset.getExecutionNodes().size() != dstDataset.getExecutionNodes().size())
            throw new IllegalStateException("different DOP of src and dst datasets");

        for (int i = 0; i < dstDataset.getExecutionNodes().size(); ++i) {

            Topology.ExecutionNode dstDatasetEN = dstDataset.getExecutionNodes().get(i);
            Topology.ExecutionNode srcDatasetEN = srcDataset.getExecutionNodes().get(i);

            if (!dstDatasetEN.getNodeDescriptor().getMachineDescriptor().address.equals(srcDatasetEN.getNodeDescriptor().getMachineDescriptor().address))
                throw new IllegalStateException("dataset partition is not co-located");

            final IWM2TMProtocol tmProtocol =
                       rpcManager.getRPCProtocolProxy(IWM2TMProtocol.class, dstDatasetEN.getNodeDescriptor().getMachineDescriptor());

            tmProtocol.assignDataset(dstDatasetEN.getNodeDescriptor().taskID, srcDatasetEN.getNodeDescriptor().taskID);
        }
    }

    @Override
    public <E> Collection<E> getBroadcastDataset(final UUID datasetID) {
        return environmentManager.getBroadcastDataset(datasetID);
    }

    // ---------------------------------------------------

    @Override
    public InputSplit requestNextInputSplit(final UUID topologyID, final UUID taskID, int sequenceNumber) {
        final AuraTopology topology = this.registeredTopologies.get(topologyID).getTopology();
        final Topology.ExecutionNode exNode = topology.executionNodeMap.get(taskID);
        return infrastructureManager.getNextInputSplitForHDFSSource(exNode);
    }

    // ---------------------------------------------------

    @Override
    public void doNextIteration(final UUID topologyID, final UUID taskID) {
        final TopologyController tc = this.registeredTopologies.get(topologyID);
        tc.evaluateIteration(taskID);
    }

    // ---------------------------------------------------

    @Override
    public List<TaskManagerStatus> getClusterUtilization() {
        final List<TaskManagerStatus> taskManagerStatuses = new ArrayList<>();
        for (final MachineDescriptor md : infrastructureManager.getTaskManagerMachines().values()) {
            final IWM2TMProtocol tmProtocol = rpcManager.getRPCProtocolProxy(IWM2TMProtocol.class, md);
            taskManagerStatuses.add(tmProtocol.getTaskManagerStatus());
        }
        return Collections.unmodifiableList(taskManagerStatuses);
    }

    // ---------------------------------------------------
    // Public Getter Methods.
    // ---------------------------------------------------

    @Override
    public IConfig getConfig() { return this.config; }

    @Override
    public IIOManager getIOManager() {
        return this.ioManager;
    }

    @Override
    public IRPCManager getRPCManager() {
        return this.rpcManager;
    }

    @Override
    public IInfrastructureManager getInfrastructureManager() {
        return this.infrastructureManager;
    }

    @Override
    public IDistributedEnvironment getEnvironmentManager() {
        return this.environmentManager;
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        // construct base argument parser
        ArgumentParser parser = getArgumentParser();

        try {
            // parse the arguments and store them as system properties
            Namespace ns = parser.parseArgs(args);
            for (Map.Entry<String, Object> e : ns.getAttrs().entrySet()) {
                if (e.getValue() != null)
                    System.setProperty(e.getKey(), e.getValue().toString());
            }

            // start the workload manager
            long start = System.nanoTime();
            ExecutionMode mode = ns.get("aura.execution.mode");
            switch (mode) {
                case DISTRIBUTED:
                    new WorkloadManager(IConfigFactory.load(IConfig.Type.WM));
                    break;
                case LOCAL:
                    new WorkloadManager(IConfigFactory.load(IConfig.Type.WM));
                    new TaskManager(IConfigFactory.load(IConfig.Type.TM));
                    break;
            }
            LOG.info(String.format("WM startup in %s mode in %s ms", mode, Long.toString(Math.abs(System.nanoTime() - start) / 1000000)));
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
        ArgumentParser parser = ArgumentParsers.newArgumentParser("aura-wm")
                .defaultHelp(true)
                .description("AURA WorkloadManager.");

        parser.addArgument("--config-dir")
                .type(new FileArgumentType().verifyIsDirectory().verifyCanRead())
                .dest("aura.path.config")
                .setDefault("config")
                .metavar("PATH")
                .help("config folder");

        parser.addArgument("--mode")
                .type(ExecutionMode.class)
                .dest("aura.execution.mode")
                .setDefault(ExecutionMode.DISTRIBUTED)
                .metavar("MODE")
                .help("execution mode ('DISTRIBUTED' or 'LOCAL')");
        //@formatter:on

        return parser;
    }
}
