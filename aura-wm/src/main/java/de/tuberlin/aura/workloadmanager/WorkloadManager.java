package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import de.tuberlin.aura.core.protocols.ITM2WMProtocol;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;
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

    public final MachineDescriptor machineDescriptor;

    public final IIOManager ioManager;

    public final IRPCManager rpcManager;

    public final IInfrastructureManager infrastructureManager;

    public final IDistributedEnvironment environmentManager;

    private final Map<UUID, TopologyController> registeredTopologies;

    private final Map<UUID, Set<UUID>> registeredSessions;

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

        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zkServer);

        // Generate MachineDescriptor for WorkloadManager.
        this.machineDescriptor = DescriptorFactory.createMachineDescriptor(config.getConfig("wm"));
        // Initialize IOManager.
        this.ioManager = new IOManager(this.machineDescriptor, null, config.getConfig("wm.io"));

        // Register EventHandler (acts as an Dispatcher) for TaskManager state updates.
        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).dispatchEvent(event);
            }
        });
        // Register EventHandler (acts as an Dispatcher) for TaskManager transitions.
        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).getTopologyFSMDispatcher().dispatchEvent((Event) event.getPayload());
            }
        });

        // Initialize RPC Manager.
        this.rpcManager = new RPCManager(ioManager, config.getConfig("wm.io.rpc"));
        // Register RPC Protocols.
        rpcManager.registerRPCProtocol(this, IClientWMProtocol.class);
        rpcManager.registerRPCProtocol(this, ITM2WMProtocol.class);

        // Initialize InfrastructureManager.
        this.infrastructureManager = new InfrastructureManager(this, zkServer, machineDescriptor);
        // Initialize InfrastructureManager.
        this.environmentManager = new DistributedEnvironment();
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public void openSession(final UUID sessionID) {
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
        if (topology == null)
            throw new IllegalArgumentException("topology == null");
        if (registeredTopologies.containsKey(topology.topologyID))
            throw new IllegalStateException("topology already submitted");

        LOG.info("TOPOLOGY '" + topology.name + "' SUBMITTED");
        registerTopology(sessionID, topology).assembleTopology();
    }

    @Override
    public void closeSession(final UUID sessionID) {
        // Sanity check.
        /*if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");
        if (!registeredSessions.containsKey(sessionID))
            throw new IllegalStateException("session with this ID [" + sessionID.toString() + "] does not exist");

        final Set<UUID> assignedTopologies = registeredSessions.get(sessionID);
        for (final UUID topologyID : assignedTopologies) {
            final TopologyController topologyController = registeredTopologies.get(topologyID);
            //topologyController.cancelTopology(); // TODO: Not implemented yet!
            //unregisterTopology(topologyID);
        }*/

        LOG.info("CLOSED SESSION [" + sessionID + "]");
    }

    public TopologyController registerTopology(final UUID sessionID, final AuraTopology topology) {
        // Sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        // TODO: sessionID not further used at the moment.

        final TopologyController topologyController = new TopologyController(this, topology.topologyID, topology, this.config);
        registeredTopologies.put(topology.topologyID, topologyController);
        return topologyController;
    }

    public void unregisterTopology(final UUID topologyID) {
        // Sanity check.
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if (registeredTopologies.remove(topologyID) == null)
            throw new IllegalStateException("topologyID not found");

        for (final Set<UUID> assignedTopologies : registeredSessions.values()) {
            if (assignedTopologies.contains(topologyID))
                assignedTopologies.remove(topologyID);
        }
    }

    @Override
    public <E> Collection<E> getDataset(final UUID uid) {
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
    public <E> void broadcastDataset(final UUID datasetID, final Collection<E> dataset) {
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
    }

    @Override
    public <E> Collection<E> getBroadcastDataset(final UUID datasetID) {
        return environmentManager.getBroadcastDataset(datasetID);
    }

    @Override
    public InputSplit requestNextInputSplit(final UUID topologyID, final UUID taskID, int sequenceNumber) {
        final AuraTopology topology = this.registeredTopologies.get(topologyID).getTopology();
        final Topology.ExecutionNode exNode = topology.executionNodeMap.get(taskID);
        return infrastructureManager.getNextInputSplitForHDFSSource(exNode);
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
