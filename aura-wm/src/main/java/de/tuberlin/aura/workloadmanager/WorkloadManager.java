package de.tuberlin.aura.workloadmanager;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.statistic.MeasurementManager;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.zookeeper.ZookeeperHelper;


// TODO: introduce the concept of a session, that allows to submit multiple queries...

public class WorkloadManager implements ClientWMProtocol {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(WorkloadManager.class);

    private final MachineDescriptor machineDescriptor;

    private final IOManager ioManager;

    private final RPCManager rpcManager;

    private final InfrastructureManager infrastructureManager;

    private final Map<UUID, TopologyController> registeredTopologies;

    private final WorkloadManagerContext managerContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public WorkloadManager(final String zkServer, int dataPort, int controlPort) {
        this(zkServer, DescriptorFactory.createMachineDescriptor(dataPort, controlPort));
    }

    public WorkloadManager(final String zkServer, final MachineDescriptor machineDescriptor) {
        // sanity check.
        ZookeeperHelper.checkConnectionString(zkServer);
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");

        this.machineDescriptor = machineDescriptor;

        this.ioManager = new IOManager(this.machineDescriptor, null);

        this.rpcManager = new RPCManager(ioManager);

        this.infrastructureManager = InfrastructureManager.getInstance(zkServer, machineDescriptor);

        this.registeredTopologies = new ConcurrentHashMap<UUID, TopologyController>();

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

    @Override
    public void submitTopology(final AuraTopology topology) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        if (registeredTopologies.containsKey(topology.name))
            throw new IllegalStateException("topology already submitted");

        LOG.info("TOPOLOGY '" + topology.name + "' SUBMITTED");
        registerTopology(topology).assembleTopology();
    }

    public TopologyController registerTopology(final AuraTopology topology) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        final TopologyController topologyController = new TopologyController(managerContext, topology);
        registeredTopologies.put(topology.topologyID, topologyController);
        return topologyController;
    }

    public void unregisterTopology(final UUID topologyID) {
        // sanity check.
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");

        if (registeredTopologies.remove(topologyID) == null)
            throw new IllegalStateException("topologyID not found");
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final Logger rootLOG = Logger.getRootLogger();

        final PatternLayout layout = new PatternLayout("%d %p - %m%n");
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        rootLOG.addAppender(consoleAppender);
        rootLOG.setLevel(Level.INFO);

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
                System.err.println("Argument" + " must be an integer");
                System.exit(1);
            }
        } else {
            System.err.println("only two numeric arguments allowed: dataPort, controlPort");
            System.exit(1);
        }

        MeasurementManager.setRoot(measurementPath);

        long start = System.nanoTime();
        new WorkloadManager(zkServer, dataPort, controlPort);
        LOG.info("WM startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
    }
}
