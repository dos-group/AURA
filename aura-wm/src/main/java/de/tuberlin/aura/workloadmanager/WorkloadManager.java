package de.tuberlin.aura.workloadmanager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.taskmanager.TaskManager;



// TODO: introduce the concept of a session, that allows to submit multiple queries...

public class WorkloadManager implements ClientWMProtocol {

    // ---------------------------------------------------
    // Execution Modes.
    // ---------------------------------------------------

    public static enum ExecutionMode {
        local("local"),
        distributed("distributed");

        final String name;

        private ExecutionMode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(WorkloadManager.class);

    private final MachineDescriptor machineDescriptor;

    private final IOManager ioManager;

    private final RPCManager rpcManager;

    private final InfrastructureManager infrastructureManager;

    private final Map<UUID, TopologyController> registeredTopologies;

    private final Map<UUID, Set<UUID>> registeredSessions;

    private final WorkloadManagerContext managerContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public WorkloadManager(IConfig config) {
        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));

        // sanity check.
        ZookeeperClient.checkConnectionString(zkServer);

        this.machineDescriptor = DescriptorFactory.createMachineDescriptor(config.getConfig("wm"));

        this.ioManager = new IOManager(this.machineDescriptor, null, config.getConfig("wm.io"));

        this.rpcManager = new RPCManager(ioManager, config.getConfig("wm.io.rpc"));

        this.infrastructureManager = InfrastructureManager.getInstance(zkServer, machineDescriptor);

        this.registeredTopologies = new ConcurrentHashMap<>();

        this.registeredSessions = new ConcurrentHashMap<>();

        rpcManager.registerRPCProtocolImpl(this, ClientWMProtocol.class);

        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).dispatchEvent(event);
            }
        });

        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                registeredTopologies.get(event.getTopologyID()).getTopologyFSMDispatcher().dispatchEvent((Event) event.getPayload());
            }
        });

        this.managerContext = new WorkloadManagerContext(this, ioManager, rpcManager, infrastructureManager);
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     * 
     * @param sessionID
     */
    @Override
    public void openSession(final UUID sessionID) {
        // sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");

        if (registeredSessions.containsKey(sessionID))
            throw new IllegalStateException("session with this ID [" + sessionID.toString() + "] already exists");

        // register a new session for a examples.
        registeredSessions.put(sessionID, new HashSet<UUID>());
        LOG.info("OPENED SESSION [" + sessionID + "]");
    }

    /**
     * @param sessionID
     * @param topology
     */
    @Override
    public void submitTopology(final UUID sessionID, final AuraTopology topology) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        if (registeredTopologies.containsKey(topology.name))
            throw new IllegalStateException("topology already submitted");

        LOG.info("TOPOLOGY '" + topology.name + "' SUBMITTED");
        registerTopology(sessionID, topology).assembleTopology(topology);
    }

    /**
     * 
     * @param sessionID
     */
    @Override
    public void closeSession(final UUID sessionID) {
        // sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");

        if (!registeredSessions.containsKey(sessionID))
            throw new IllegalStateException("session with this ID [" + sessionID.toString() + "] does not exist");

        // register a new session for a examples.
        final Set<UUID> assignedTopologies = registeredSessions.get(sessionID);

        for (final UUID topologyID : assignedTopologies) {
            final TopologyController topologyController = registeredTopologies.get(topologyID);
            // topologyController.cancelTopology(); // TODO: Not implemented yet!
            // unregisterTopology(topologyID);
        }

        LOG.info("CLOSED SESSION [" + sessionID + "]");
    }

    /**
     *
     * @param sessionID
     * @param topologyID
     * @param topology
     */
    @Override
    public void submitToTopology(UUID sessionID, UUID topologyID, AuraTopology topology) {
        // sanity check.
        if (sessionID == null)
            throw new IllegalArgumentException("sessionID == null");
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        final TopologyController topologyController = this.registeredTopologies.get(topologyID);

        if (topologyController == null)
            throw new IllegalStateException("topologyController == null");

        topologyController.assembleTopology(topology);
    }

    /**
     *
     * @param sessionID
     * @param topologyID1
     * @param taskNodeID1
     * @param topologyID2
     * @param taskNodeID2
     */
    /*
     * @Override public void connectTopologies(final UUID sessionID, final UUID topologyID1, final
     * UUID taskNodeID1, final UUID topologyID2, final UUID taskNodeID2) { // sanity check.
     * if(topologyID1 == null) throw new IllegalArgumentException("topologyID1 == null");
     * if(taskNodeID1 == null) throw new IllegalArgumentException("taskNodeID1 == null");
     * if(topologyID2 == null) throw new IllegalArgumentException("topologyID2 == null");
     * if(taskNodeID2 == null) throw new IllegalArgumentException("taskNodeID2 == null");
     * 
     * 
     * final TopologyController topologyControllerSrc = this.registeredTopologies.get(topologyID1);
     * 
     * if(topologyController == null) throw new IllegalStateException("topologyController == null");
     * 
     * topologyController.createOutputGateAndConnect(taskNodeID1); }
     */

    /**
     * @param sessionID
     * @param topology
     * @return
     */
    public TopologyController registerTopology(final UUID sessionID, final AuraTopology topology) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        final TopologyController topologyController = new TopologyController(managerContext, topology.topologyID);
        registeredTopologies.put(topology.topologyID, topologyController);
        return topologyController;
    }

    /**
     * 
     * @param topologyID
     */
    public void unregisterTopology(final UUID topologyID) {
        // sanity check.
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");

        if (registeredTopologies.remove(topologyID) == null)
            throw new IllegalStateException("topologyID not found");

        for (final Set<UUID> assignedTopologies : registeredSessions.values()) {
            if (assignedTopologies.contains(topologyID))
                assignedTopologies.remove(topologyID);
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
            Namespace ns = parser.parseArgs(args);
            for (Map.Entry<String, Object> e : ns.getAttrs().entrySet()) {
                if (e.getValue() != null)
                    System.setProperty(e.getKey(), e.getValue().toString());
            }

            // start the workload manager
            long start = System.nanoTime();
            ExecutionMode mode = ns.get("aura.execution.mode");
            switch (mode) {
                case distributed:
                    new WorkloadManager(IConfigFactory.load(IConfig.Type.WM));
                    break;
                case local:
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
                .setDefault(ExecutionMode.distributed)
                .metavar("MODE")
                .help("execution mode ('distributed' or 'local')");
        //@formatter:on

        return parser;
    }
}
